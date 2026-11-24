package com.systemdesign.bankingtransfer.ledgerworker;

import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Verification endpoints — the point of the topic: don't trust, reconcile.
 *
 * <p>{@code GET /api/reconciliation} compares, per account, three numbers that must agree: the
 * stored {@code balance}, the signed sum of the source {@code ledger}, and the signed sum of the
 * Kafka-projected {@code ledger_audit}. {@code drift = balance - ledgerSum} catches a write path
 * bug; {@code projectionDrift = projectionSum - ledgerSum} catches a lost/duplicated event — but
 * only after the relay + consumer have caught up, since the projection is eventually consistent by
 * design (lag is expected, drift is not).
 */
@RestController
@RequestMapping("/api")
class ReconciliationController {

  private final JdbcClient jdbc;

  ReconciliationController(JdbcClient jdbc, MeterRegistry registry) {
    this.jdbc = jdbc;
    // Recomputed on every scrape; a non-zero value means money in accounts and
    // money in the ledger disagree — the alarm this whole exercise exists to raise.
    registry.gauge("reconciliation.drift", this, ReconciliationController::totalAbsDrift);
  }

  @GetMapping("/reconciliation")
  List<AccountReconciliation> reconciliation() {
    return jdbc.sql("SELECT id, balance FROM accounts ORDER BY id")
        .query(
            (rs, rowNum) -> {
              long id = rs.getLong("id");
              BigDecimal balance = rs.getBigDecimal("balance");
              BigDecimal ledgerSum = signedSum("ledger", id);
              BigDecimal projectionSum = signedSum("ledger_audit", id);
              return new AccountReconciliation(
                  id,
                  balance,
                  ledgerSum,
                  projectionSum,
                  balance.subtract(ledgerSum),
                  projectionSum.subtract(ledgerSum));
            })
        .list();
  }

  /**
   * The eventually-consistent view of an account's entries as seen through Kafka. It can lag the
   * source {@code /api/accounts/{id}/statement} by a relay tick — that lag is the lesson, not a
   * bug.
   */
  @GetMapping("/accounts/{id}/audit")
  ResponseEntity<List<AuditEntry>> audit(@PathVariable long id) {
    boolean exists =
        jdbc.sql("SELECT 1 FROM accounts WHERE id = :id")
            .param("id", id)
            .query(Integer.class)
            .optional()
            .isPresent();
    if (!exists) {
      return ResponseEntity.notFound().build();
    }
    return ResponseEntity.ok(
        jdbc.sql(
                "SELECT transfer_id, entry, amount, balance_after, at FROM ledger_audit"
                    + " WHERE account_id = :id ORDER BY id")
            .param("id", id)
            .query(
                (rs, rowNum) ->
                    new AuditEntry(
                        rs.getLong("transfer_id"),
                        rs.getString("entry"),
                        rs.getBigDecimal("amount"),
                        rs.getBigDecimal("balance_after"),
                        rs.getObject("at", OffsetDateTime.class)))
            .list());
  }

  private BigDecimal signedSum(String table, long accountId) {
    return jdbc.sql(
            "SELECT COALESCE(SUM(CASE WHEN entry = 'CREDIT' THEN amount ELSE -amount END), 0)"
                + " FROM "
                + table
                + " WHERE account_id = :id")
        .param("id", accountId)
        .query(BigDecimal.class)
        .single();
  }

  private double totalAbsDrift() {
    return reconciliation().stream().mapToDouble(r -> r.drift().abs().doubleValue()).sum();
  }

  record AccountReconciliation(
      long accountId,
      BigDecimal balance,
      BigDecimal ledgerSum,
      BigDecimal projectionSum,
      BigDecimal drift,
      BigDecimal projectionDrift) {}

  record AuditEntry(
      long transferId,
      String entry,
      BigDecimal amount,
      BigDecimal balanceAfter,
      OffsetDateTime at) {}
}
