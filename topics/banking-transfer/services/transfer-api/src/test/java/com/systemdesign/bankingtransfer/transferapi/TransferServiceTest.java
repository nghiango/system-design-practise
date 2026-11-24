package com.systemdesign.bankingtransfer.transferapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;

class TransferServiceTest {

  private JdbcClient jdbc;
  private TransferService service;

  /** sql fragment → every completed param-map passed to .update() on statements matching it. */
  private final Map<String, List<Map<String, Object>>> executedUpdates = new LinkedHashMap<>();

  /** Param values passed to FOR UPDATE lock statements, in call order. */
  private final List<Long> lockOrder = new ArrayList<>();

  // Test-controlled stub outcomes (set per test before calling the service).
  private Long insertReturnId; // null → ON CONFLICT fired → replay path
  private BigDecimal lockBalance; // null → locked row absent → account not found
  private TransferService.TransferView storedTransfer; // replay SELECT result

  @BeforeEach
  void setUp() {
    jdbc = mock(JdbcClient.class, Mockito.RETURNS_DEEP_STUBS);
    service = new TransferService(jdbc);
    insertReturnId = 1L;
    lockBalance = new BigDecimal("100.00");
    storedTransfer =
        new TransferService.TransferView(7L, "COMPLETED", 1L, 2L, new BigDecimal("10"));
    // One spec factory for every statement: records params per executed UPDATE,
    // records lock order on FOR UPDATE, returns the test-configured query results.
    when(jdbc.sql(anyString())).thenAnswer(inv -> recordingSpec(inv.getArgument(0)));
  }

  private JdbcClient.StatementSpec recordingSpec(String sql) {
    JdbcClient.StatementSpec spec =
        mock(JdbcClient.StatementSpec.class, Mockito.RETURNS_DEEP_STUBS);
    Map<String, Object> current = new LinkedHashMap<>();
    when(spec.param(anyString(), any()))
        .thenAnswer(
            p -> {
              Object value = p.getArgument(1);
              current.put(p.getArgument(0), value);
              if (sql.contains("FOR UPDATE")) {
                lockOrder.add((Long) value);
              }
              return spec;
            });
    when(spec.param(anyString(), any(), anyInt()))
        .thenAnswer(
            p -> {
              current.put(p.getArgument(0), p.getArgument(1));
              return spec;
            });
    when(spec.update())
        .thenAnswer(
            u -> {
              executedUpdates.computeIfAbsent(sql, k -> new ArrayList<>()).add(Map.copyOf(current));
              current.clear();
              return 1;
            });
    @SuppressWarnings("unchecked")
    JdbcClient.MappedQuerySpec<Long> longQuery = mock(JdbcClient.MappedQuerySpec.class);
    when(longQuery.optional()).thenAnswer(o -> Optional.ofNullable(insertReturnId));
    when(spec.query(Long.class)).thenReturn(longQuery);
    @SuppressWarnings("unchecked")
    JdbcClient.MappedQuerySpec<BigDecimal> decimalQuery = mock(JdbcClient.MappedQuerySpec.class);
    when(decimalQuery.optional()).thenAnswer(o -> Optional.ofNullable(lockBalance));
    when(spec.query(BigDecimal.class)).thenReturn(decimalQuery);
    @SuppressWarnings("unchecked")
    JdbcClient.MappedQuerySpec<Object> rowQuery = mock(JdbcClient.MappedQuerySpec.class);
    when(rowQuery.optional()).thenAnswer(o -> Optional.ofNullable(storedTransfer));
    when(spec.query(any(RowMapper.class))).thenReturn((JdbcClient.MappedQuerySpec) rowQuery);
    return spec;
  }

  private List<Map<String, Object>> updatesMatching(String fragment) {
    return executedUpdates.entrySet().stream()
        .filter(e -> e.getKey().contains(fragment))
        .flatMap(e -> e.getValue().stream())
        .toList();
  }

  @Test
  void replayReturnsStoredResultAndNeverLocksOrWritesLedger() {
    insertReturnId = null; // ON CONFLICT fired — a stored row already exists

    TransferService.TransferOutcome outcome = service.transfer("k-1", 1L, 2L, new BigDecimal("10"));

    assertThat(outcome.result()).isEqualTo(TransferService.TransferResult.REPLAYED);
    assertThat(outcome.transfer().id()).isEqualTo(7L);
    assertThat(outcome.transfer().status()).isEqualTo("COMPLETED");
    // The whole point: a replay must not touch balances, ledger or outbox again.
    verify(jdbc, never()).sql(contains("FOR UPDATE"));
    verify(jdbc, never()).sql(contains("INSERT INTO ledger"));
    verify(jdbc, never()).sql(contains("INSERT INTO outbox"));
  }

  @Test
  void insufficientFundsWritesRejectedTransferAndOutboxButNoLedger() {
    lockBalance = new BigDecimal("5.00");

    TransferService.TransferOutcome outcome =
        service.transfer("k-2", 1L, 2L, new BigDecimal("10.00"));

    assertThat(outcome.result()).isEqualTo(TransferService.TransferResult.REJECTED);
    assertThat(outcome.transfer().status()).isEqualTo("REJECTED");

    List<Map<String, Object>> outboxRows = updatesMatching("INSERT INTO outbox");
    assertThat(outboxRows).hasSize(1);
    assertThat(outboxRows.get(0).get("type")).isEqualTo("transfer.rejected");
    assertThat((String) outboxRows.get(0).get("payload")).contains("insufficient-funds");

    verify(jdbc).sql(contains("SET status = 'REJECTED'"));
    verify(jdbc, never()).sql(contains("INSERT INTO ledger"));
    verify(jdbc, never()).sql(contains("UPDATE accounts"));
  }

  @Test
  void accountsAreLockedInAscendingIdOrderRegardlessOfDirection() {
    // from=2, to=1: without ordering a txn would lock 2 then 1 — the deadlock shape.
    service.transfer("k-3", 2L, 1L, new BigDecimal("10.00"));

    assertThat(lockOrder).containsExactly(1L, 2L);
  }

  @Test
  void completedTransferUpdatesBalancesLedgerPairStatusAndOutbox() {
    insertReturnId = 11L;

    TransferService.TransferOutcome outcome =
        service.transfer("k-4", 1L, 2L, new BigDecimal("30.00"));

    assertThat(outcome.result()).isEqualTo(TransferService.TransferResult.COMPLETED);

    // Debit side loses 30, credit side gains 30.
    List<Map<String, Object>> accountUpdates = updatesMatching("UPDATE accounts");
    assertThat(accountUpdates).hasSize(2);
    assertThat(accountUpdates.get(0))
        .containsEntry("id", 1L)
        .containsEntry("amount", new BigDecimal("30.00"));
    assertThat(accountUpdates.get(1)).containsEntry("id", 2L);

    // Two ledger rows: DEBIT on from (balance 70), CREDIT on to (balance 130).
    List<Map<String, Object>> ledgerRows = updatesMatching("INSERT INTO ledger");
    assertThat(ledgerRows).hasSize(2);
    assertThat(ledgerRows.get(0))
        .containsEntry("transferId", 11L)
        .containsEntry("accountId", 1L)
        .containsEntry("entry", "DEBIT")
        .containsEntry("balanceAfter", new BigDecimal("70.00"));
    assertThat(ledgerRows.get(1))
        .containsEntry("accountId", 2L)
        .containsEntry("entry", "CREDIT")
        .containsEntry("balanceAfter", new BigDecimal("130.00"));

    List<Map<String, Object>> outboxRows = updatesMatching("INSERT INTO outbox");
    assertThat(outboxRows).hasSize(1);
    assertThat(outboxRows.get(0).get("type")).isEqualTo("transfer.completed");
    assertThat((String) outboxRows.get(0).get("payload")).contains("\"transferId\":11");

    verify(jdbc).sql(contains("SET status = 'COMPLETED'"));
  }

  @Test
  void missingAccountAbortsTheWholeTransaction() {
    lockBalance = null; // first lock comes back empty → 404, INITIATED row rolls back

    assertThatThrownBy(() -> service.transfer("k-5", 1L, 2L, new BigDecimal("1.00")))
        .isInstanceOf(TransferService.AccountNotFoundException.class);
  }
}
