package com.systemdesign.ticketbooking.bookingworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;

class ExpirySweeperTest {

  private JdbcClient jdbc;
  private KafkaTemplate<String, String> kafka;
  private SimpleMeterRegistry registry;
  private ExpirySweeper sweeper;

  @BeforeEach
  void setUp() {
    jdbc = mock(JdbcClient.class, Mockito.RETURNS_DEEP_STUBS);
    kafka = mock(KafkaTemplate.class);
    registry = new SimpleMeterRegistry();
    sweeper = new ExpirySweeper(jdbc, kafka, registry);
  }

  @Test
  @SuppressWarnings("unchecked")
  void sweepReclaimsEveryDeadHoldInOneGuardedUpdate() {
    when(jdbc.sql(contains("hold_expires_at < now()")).query(any(RowMapper.class)).list())
        .thenReturn(
            List.of(
                new ExpirySweeper.ReleasedSeat(7L, "A1", "old-1"),
                new ExpirySweeper.ReleasedSeat(7L, "B2", "old-2")));

    sweeper.sweep();

    // One UPDATE, guarded by status='HELD' and expiry — the row lock makes reclaim atomic.
    verify(jdbc).sql(contains("WHERE status = 'HELD' AND hold_expires_at < now()"));
    verify(jdbc).sql(contains("RETURNING event_id, seat, hold_owner"));
    verify(kafka)
        .send(
            ExpirySweeper.TOPIC,
            "7:A1",
            "{\"type\":\"EXPIRED\",\"eventId\":7,\"seat\":\"A1\",\"owner\":\"old-1\"}");
    verify(kafka)
        .send(
            ExpirySweeper.TOPIC,
            "7:B2",
            "{\"type\":\"EXPIRED\",\"eventId\":7,\"seat\":\"B2\",\"owner\":\"old-2\"}");
    assertThat(registry.counter("seats.expired").count()).isEqualTo(2.0);
  }

  @Test
  @SuppressWarnings("unchecked")
  void sweepWithNothingExpiredPublishesNothing() {
    when(jdbc.sql(contains("hold_expires_at < now()")).query(any(RowMapper.class)).list())
        .thenReturn(List.of());

    sweeper.sweep();

    verify(kafka, Mockito.never()).send(any(), any(), any());
    assertThat(registry.counter("seats.expired").count()).isEqualTo(0.0);
  }
}
