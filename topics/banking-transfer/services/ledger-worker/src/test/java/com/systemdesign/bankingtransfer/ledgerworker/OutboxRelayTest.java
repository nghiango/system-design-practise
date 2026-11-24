package com.systemdesign.bankingtransfer.ledgerworker;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.ObjectMapper;

class OutboxRelayTest {

  private static final String CLAIM_SQL =
      "SELECT id, payload FROM outbox WHERE NOT published"
          + " ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED";
  private static final String MARK_SQL = "UPDATE outbox SET published = true WHERE id = :id";

  private JdbcClient jdbc;
  private KafkaTemplate<String, String> kafka;
  private OutboxRelay relay;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    jdbc = mock(JdbcClient.class, Mockito.RETURNS_DEEP_STUBS);
    kafka = mock(KafkaTemplate.class);
    relay = new OutboxRelay(jdbc, kafka, new ObjectMapper(), new SimpleMeterRegistry());
  }

  private void pendingRows(List<OutboxRelay.OutboxRow> rows) {
    when(jdbc.sql(contains("FOR UPDATE SKIP LOCKED")).query(any(RowMapper.class)).list())
        .thenReturn(rows);
  }

  private void sendSucceeds() {
    when(kafka.send(anyString(), anyString(), anyString()))
        .thenReturn(CompletableFuture.<SendResult<String, String>>completedFuture(null));
  }

  @Test
  void relayPublishesPayloadKeyedByTransferIdThenMarksPublished() {
    pendingRows(
        List.of(
            new OutboxRelay.OutboxRow(1L, "{\"type\":\"transfer.completed\",\"transferId\":7}")));
    sendSucceeds();

    relay.relay();

    verify(kafka).send(eq(OutboxRelay.TOPIC), eq("7"), contains("transferId\":7"));
    // The deep-stub mock for the exact mark-published statement is the same
    // instance the relay used — verifying it verifies the update ran.
    // Deep stubs: param() returns the mock the real fluent chain calls update() on.
    JdbcClient.StatementSpec markSpec = jdbc.sql(MARK_SQL);
    verify(markSpec.param("id", 1L)).update();
  }

  @Test
  void sendFailureRollsBackNothingIsMarkedPublished() {
    pendingRows(List.of(new OutboxRelay.OutboxRow(2L, "{\"transferId\":8}")));
    CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RuntimeException("broker down"));
    when(kafka.send(anyString(), anyString(), anyString())).thenReturn(failed);

    assertThatThrownBy(() -> relay.relay()).isInstanceOf(OutboxRelay.RelayException.class);
    // The claim SELECT ran, but the row was never marked published — next tick retries it.
    verify(jdbc).sql(contains("FOR UPDATE SKIP LOCKED"));
    verify(jdbc, never()).sql(MARK_SQL);
  }

  @Test
  void emptyBatchSendsNothing() {
    pendingRows(List.of());
    relay.relay();
    verify(kafka, never()).send(anyString(), anyString(), anyString());
    verify(jdbc, never()).sql(MARK_SQL);
  }
}
