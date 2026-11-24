package com.systemdesign.bankingtransfer.transferapi;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Money movement as a single Postgres transaction (decision 001).
 *
 * <p>{@link #transfer} is ONE {@code @Transactional} unit: claim the idempotency key, lock both
 * account rows in ascending id order (deadlock-safe), check funds, update balances, write the
 * double-entry ledger pair and the outbox row. Commit = all of it; rollback = none of it. The event
 * can never be lost without the transfer also being lost, and a retried request can never
 * double-post because the idempotency key unique index arbitrates before any lock is taken.
 */
@Service
class TransferService {

  private final JdbcClient jdbc;

  TransferService(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  /** Thrown for unknown accounts; rolls the INITIATED row back with the transaction. */
  static class AccountNotFoundException extends RuntimeException {
    AccountNotFoundException(long id) {
      super("unknown account " + id);
    }
  }

  // ---- accounts ---------------------------------------------------------------------------

  /**
   * Creates an account and — when {@code initial} is positive — books its opening CREDIT ledger
   * entry plus an {@code account.opened} outbox event in the same transaction. The opening entry is
   * what makes {@code Σ signed entries == balance} hold from the very first row, so reconciliation
   * can start at zero drift instead of a permanent "seed money" gap. The ledger row has {@code
   * transfer_id NULL}; the projected copy carries transfer id {@code 0}.
   */
  @Transactional
  long createAccount(String owner, BigDecimal initial) {
    long id =
        jdbc.sql("INSERT INTO accounts (owner, balance) VALUES (:owner, :initial) RETURNING id")
            .param("owner", owner)
            .param("initial", initial)
            .query(Long.class)
            .single();
    if (initial.signum() > 0) {
      insertLedger(null, id, "CREDIT", initial, initial);
      insertOutbox(
          "account.opened",
          "{\"type\":\"account.opened\",\"transferId\":0,\"accountId\":"
              + id
              + ",\"entries\":["
              + entryJson(id, "CREDIT", initial, initial)
              + "]}");
    }
    return id;
  }

  Optional<AccountView> account(long id) {
    return jdbc.sql("SELECT id, owner, balance FROM accounts WHERE id = :id")
        .param("id", id)
        .query(
            (rs, rowNum) ->
                new AccountView(
                    rs.getLong("id"), rs.getString("owner"), rs.getBigDecimal("balance")))
        .optional();
  }

  /** The account's ledger entries, oldest first — the statement view. */
  List<LedgerEntryView> statement(long accountId) {
    return jdbc.sql(
            "SELECT transfer_id, entry, amount, balance_after, created_at FROM ledger"
                + " WHERE account_id = :id ORDER BY id")
        .param("id", accountId)
        .query(
            (rs, rowNum) ->
                new LedgerEntryView(
                    rs.getLong("transfer_id"),
                    rs.getString("entry"),
                    rs.getBigDecimal("amount"),
                    rs.getBigDecimal("balance_after"),
                    rs.getObject("created_at", java.time.OffsetDateTime.class)))
        .list();
  }

  boolean accountExists(long id) {
    return jdbc.sql("SELECT 1 FROM accounts WHERE id = :id")
        .param("id", id)
        .query(Integer.class)
        .optional()
        .isPresent();
  }

  // ---- transfers ----------------------------------------------------------------------------

  Optional<TransferView> transfer(long id) {
    return jdbc.sql(
            "SELECT id, status, from_account, to_account, amount FROM transfers WHERE id = :id")
        .param("id", id)
        .query(this::mapTransfer)
        .optional();
  }

  Optional<TransferView> transferByKey(String key) {
    return jdbc.sql(
            "SELECT id, status, from_account, to_account, amount FROM transfers"
                + " WHERE idempotency_key = :key")
        .param("key", key)
        .query(this::mapTransfer)
        .optional();
  }

  private TransferView mapTransfer(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
    return new TransferView(
        rs.getLong("id"),
        rs.getString("status"),
        rs.getLong("from_account"),
        rs.getLong("to_account"),
        rs.getBigDecimal("amount"));
  }

  /**
   * Executes a transfer in one transaction.
   *
   * <ol>
   *   <li>{@code INSERT ... ON CONFLICT (idempotency_key) DO NOTHING} — the unique index is the
   *       single-writer gate. A conflict returns the stored row unchanged: the replayed response is
   *       byte-identical to the first one and no second ledger pair is ever written.
   *   <li>{@code SELECT ... FOR UPDATE} on both accounts <em>in ascending id order</em>. Two
   *       opposite-direction transfers (A→B and B→A) then grab the same locks in the same order, so
   *       the second waits instead of deadlocking.
   *   <li>Insufficient funds → {@code REJECTED} row + {@code transfer.rejected} outbox event, still
   *       inside the same transaction, so even a rejection is durable and auditable.
   *   <li>Otherwise both balances move, the DEBIT/CREDIT ledger pair records the new {@code
   *       balance_after} on each side, the row goes {@code COMPLETED}, and the {@code
   *       transfer.completed} outbox row is queued for the relay.
   * </ol>
   */
  @Transactional
  TransferOutcome transfer(String key, long from, long to, BigDecimal amount) {
    Optional<Long> inserted =
        jdbc.sql(
                "INSERT INTO transfers (idempotency_key, from_account, to_account, amount)"
                    + " VALUES (:key, :from, :to, :amount)"
                    + " ON CONFLICT (idempotency_key) DO NOTHING RETURNING id")
            .param("key", key)
            .param("from", from)
            .param("to", to)
            .param("amount", amount)
            .query(Long.class)
            .optional();
    if (inserted.isEmpty()) {
      // Replay: read what the winning transaction stored and return it verbatim.
      return new TransferOutcome(TransferResult.REPLAYED, transferByKey(key).orElseThrow());
    }
    long transferId = inserted.get();

    // Lock order is by account id, NOT by which side of the transfer the account is on —
    // opposite-direction transfers then lock the same rows in the same sequence and a cycle
    // (txn1 holds A waits B, txn2 holds B waits A) cannot form.
    long firstId = Math.min(from, to);
    long secondId = Math.max(from, to);
    BigDecimal firstBalance = lockForUpdate(firstId);
    BigDecimal secondBalance = lockForUpdate(secondId);
    BigDecimal fromBalance = (from == firstId) ? firstBalance : secondBalance;
    BigDecimal toBalance = (from == firstId) ? secondBalance : firstBalance;

    if (fromBalance.compareTo(amount) < 0) {
      jdbc.sql("UPDATE transfers SET status = 'REJECTED' WHERE id = :id")
          .param("id", transferId)
          .update();
      insertOutbox(
          "transfer.rejected",
          "{\"type\":\"transfer.rejected\",\"transferId\":"
              + transferId
              + ",\"from\":"
              + from
              + ",\"to\":"
              + to
              + ",\"amount\":"
              + amount
              + ",\"reason\":\"insufficient-funds\"}");
      return new TransferOutcome(
          TransferResult.REJECTED, new TransferView(transferId, "REJECTED", from, to, amount));
    }

    BigDecimal newFromBalance = fromBalance.subtract(amount);
    BigDecimal newToBalance = toBalance.add(amount);
    jdbc.sql("UPDATE accounts SET balance = balance - :amount WHERE id = :id")
        .param("amount", amount)
        .param("id", from)
        .update();
    jdbc.sql("UPDATE accounts SET balance = balance + :amount WHERE id = :id")
        .param("amount", amount)
        .param("id", to)
        .update();
    insertLedger(transferId, from, "DEBIT", amount, newFromBalance);
    insertLedger(transferId, to, "CREDIT", amount, newToBalance);
    jdbc.sql("UPDATE transfers SET status = 'COMPLETED' WHERE id = :id")
        .param("id", transferId)
        .update();
    insertOutbox(
        "transfer.completed",
        "{\"type\":\"transfer.completed\",\"transferId\":"
            + transferId
            + ",\"from\":"
            + from
            + ",\"to\":"
            + to
            + ",\"amount\":"
            + amount
            + ",\"entries\":["
            + entryJson(from, "DEBIT", amount, newFromBalance)
            + ","
            + entryJson(to, "CREDIT", amount, newToBalance)
            + "]}");
    return new TransferOutcome(
        TransferResult.COMPLETED, new TransferView(transferId, "COMPLETED", from, to, amount));
  }

  private BigDecimal lockForUpdate(long accountId) {
    return jdbc.sql("SELECT balance FROM accounts WHERE id = :id FOR UPDATE")
        .param("id", accountId)
        .query(BigDecimal.class)
        .optional()
        .orElseThrow(() -> new AccountNotFoundException(accountId));
  }

  private void insertLedger(
      Long transferId, long accountId, String entry, BigDecimal amount, BigDecimal balanceAfter) {
    jdbc.sql(
            "INSERT INTO ledger (transfer_id, account_id, entry, amount, balance_after)"
                + " VALUES (:transferId, :accountId, :entry, :amount, :balanceAfter)")
        .param("transferId", transferId, java.sql.Types.BIGINT)
        .param("accountId", accountId)
        .param("entry", entry)
        .param("amount", amount)
        .param("balanceAfter", balanceAfter)
        .update();
  }

  private void insertOutbox(String type, String payload) {
    jdbc.sql("INSERT INTO outbox (type, payload) VALUES (:type, :payload)")
        .param("type", type)
        .param("payload", payload)
        .update();
  }

  private static String entryJson(
      long accountId, String entry, BigDecimal amount, BigDecimal balanceAfter) {
    return "{\"accountId\":"
        + accountId
        + ",\"entry\":\""
        + entry
        + "\",\"amount\":"
        + amount
        + ",\"balanceAfter\":"
        + balanceAfter
        + "}";
  }

  record AccountView(long id, String owner, BigDecimal balance) {}

  record TransferView(long id, String status, long from, long to, BigDecimal amount) {}

  record LedgerEntryView(
      long transferId,
      String entry,
      java.math.BigDecimal amount,
      java.math.BigDecimal balanceAfter,
      java.time.OffsetDateTime at) {}

  enum TransferResult {
    COMPLETED,
    REJECTED,
    REPLAYED
  }

  record TransferOutcome(TransferResult result, TransferView transfer) {}
}
