package com.systemdesign.bankingtransfer.ledgerworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.ObjectMapper;

class LedgerConsumerTest {

  private JdbcClient jdbc;
  private LedgerConsumer consumer;
  private final Map<String, List<Map<String, Object>>> executed = new LinkedHashMap<>();

  @BeforeEach
  void setUp() {
    jdbc = mock(JdbcClient.class, Mockito.RETURNS_DEEP_STUBS);
    consumer = new LedgerConsumer(jdbc, new ObjectMapper());
    when(jdbc.sql(anyString())).thenAnswer(inv -> recordingSpec(inv.getArgument(0)));
  }

  private JdbcClient.StatementSpec recordingSpec(String sql) {
    JdbcClient.StatementSpec spec =
        mock(JdbcClient.StatementSpec.class, Mockito.RETURNS_DEEP_STUBS);
    Map<String, Object> current = new LinkedHashMap<>();
    when(spec.param(anyString(), any()))
        .thenAnswer(
            p -> {
              current.put(p.getArgument(0), p.getArgument(1));
              return spec;
            });
    when(spec.update())
        .thenAnswer(
            u -> {
              executed.computeIfAbsent(sql, k -> new ArrayList<>()).add(Map.copyOf(current));
              current.clear();
              return 1;
            });
    return spec;
  }

  private List<Map<String, Object>> auditInserts() {
    return executed.entrySet().stream()
        .filter(e -> e.getKey().contains("INSERT INTO ledger_audit"))
        .flatMap(e -> e.getValue().stream())
        .toList();
  }

  @Test
  void completedEventProjectsBothEntriesWithDedupeConflict() {
    consumer.onEvent(
        "{\"type\":\"transfer.completed\",\"transferId\":5,\"from\":1,\"to\":2,\"amount\":10.00,"
            + "\"entries\":["
            + "{\"accountId\":1,\"entry\":\"DEBIT\",\"amount\":10.00,\"balanceAfter\":90.00},"
            + "{\"accountId\":2,\"entry\":\"CREDIT\",\"amount\":10.00,\"balanceAfter\":110.00}]}");

    List<Map<String, Object>> rows = auditInserts();
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0))
        .containsEntry("transferId", 5L)
        .containsEntry("accountId", 1L)
        .containsEntry("entry", "DEBIT")
        .containsEntry("amount", new BigDecimal("10.0"))
        .containsEntry("balanceAfter", new BigDecimal("90.0"));
    assertThat(rows.get(1))
        .containsEntry("accountId", 2L)
        .containsEntry("entry", "CREDIT")
        .containsEntry("balanceAfter", new BigDecimal("110.0"));
    // Every insert is the dedupe shape — at-least-once delivery replays a no-op.
    assertThat(executed.keySet()).allMatch(sql -> sql.contains("ON CONFLICT"));
  }

  @Test
  void redeliveredEventRunsTheSameIdempotentInsert() {
    String event =
        "{\"type\":\"transfer.completed\",\"transferId\":5,\"entries\":["
            + "{\"accountId\":1,\"entry\":\"DEBIT\",\"amount\":10.00,\"balanceAfter\":90.00}]}";
    consumer.onEvent(event);
    consumer.onEvent(event); // at-least-once redelivery → same DO NOTHING insert, no-op in DB
    assertThat(auditInserts()).hasSize(2);
    assertThat(auditInserts().get(0)).isEqualTo(auditInserts().get(1));
  }

  @Test
  void rejectedTransferEventHasNoEntriesAndWritesNothing() {
    consumer.onEvent("{\"type\":\"transfer.rejected\",\"transferId\":9,\"reason\":\"x\"}");
    assertThat(auditInserts()).isEmpty();
  }

  @Test
  void malformedEventIsDroppedNotWedged() {
    consumer.onEvent("not json at all");
    verify(jdbc, never()).sql(anyString());
  }
}
