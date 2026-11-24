package com.systemdesign.notification.senderworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

class SendConsumerTest {

  private JdbcClient jdbc;
  private JdbcClient.StatementSpec spec;
  private JdbcClient.MappedQuerySpec<SendConsumer.Row> query;
  private KafkaTemplate<String, String> kafka;
  private RateLimiter limiter;
  private ChannelSender sender;
  private SimpleMeterRegistry registry;
  private SendConsumer consumer;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    jdbc = mock(JdbcClient.class, Mockito.RETURNS_DEEP_STUBS);
    spec = mock(JdbcClient.StatementSpec.class);
    query = mock(JdbcClient.MappedQuerySpec.class);
    kafka = mock(KafkaTemplate.class);
    limiter = mock(RateLimiter.class);
    sender = mock(ChannelSender.class);
    registry = new SimpleMeterRegistry();
    consumer = new SendConsumer(jdbc, kafka, limiter, sender, new ObjectMapper(), registry);

    when(jdbc.sql(anyString())).thenReturn(spec);
    when(spec.param(anyString(), any())).thenReturn(spec);
    when(spec.query(any(RowMapper.class))).thenReturn(query);
    when(kafka.send(anyString(), anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(limiter.tryAcquire(anyString())).thenReturn(true);
    when(spec.update()).thenReturn(1);
  }

  private void queuedRow() {
    queuedRow("QUEUED");
  }

  private void queuedRow(String status) {
    when(query.optional())
        .thenReturn(
            Optional.of(new SendConsumer.Row(1L, "email", "user@example.com", "hi", status)));
  }

  @Test
  void extractsSendEvent() {
    SendConsumer.SendEvent e = consumer.extract("{\"id\":7,\"attempt\":2}");

    assertThat(e.id()).isEqualTo(7L);
    assertThat(e.attempt()).isEqualTo(2);
  }

  @Test
  void ignoresMalformedMessages() {
    for (String bad :
        new String[] {"", "not json", "{}", "{\"id\":\"x\",\"attempt\":0}", "{\"id\":1}", "null"}) {
      assertThat(consumer.extract(bad)).isNull();
    }
    consumer.onEvent("garbage");
    verify(jdbc, never()).sql(anyString());
  }

  @Test
  void successMarksSent() {
    queuedRow();
    when(sender.send("email", "user@example.com", "hi")).thenReturn(ChannelSender.Outcome.SUCCESS);

    consumer.onEvent("{\"id\":1,\"attempt\":0}");

    verify(jdbc).sql(contains("SET status = 'SENDING'"));
    verify(jdbc).sql(contains("SET status = 'SENT'"));
    assertThat(registry.counter("notifications.sent").count()).isEqualTo(1.0);
    verify(kafka, never()).send(eq(SendConsumer.RETRY_TOPIC), anyString(), anyString());
  }

  @Test
  void throttledRequeuesWithoutConsumingAttempt() {
    queuedRow();
    when(limiter.tryAcquire("email")).thenReturn(false);

    consumer.onEvent("{\"id\":1,\"attempt\":1}");

    // Same attempt republished, row untouched (stays QUEUED), provider never called.
    verify(kafka).send(SendConsumer.RETRY_TOPIC, "1", "{\"id\":1,\"attempt\":1}");
    verify(jdbc, never()).sql(contains("UPDATE"));
    verify(sender, never()).send(anyString(), anyString(), anyString());
  }

  @Test
  void transientFailureRequeuesAttemptPlusOne() {
    when(query.optional())
        .thenReturn(Optional.of(new SendConsumer.Row(1L, "sms", "flaky-device", "hi", "QUEUED")));
    when(sender.send("sms", "flaky-device", "hi")).thenReturn(ChannelSender.Outcome.TRANSIENT);

    consumer.onEvent("{\"id\":1,\"attempt\":0}");

    verify(jdbc).sql(contains("SET status = 'QUEUED', attempts = :attempts"));
    verify(kafka).send(SendConsumer.RETRY_TOPIC, "1", "{\"id\":1,\"attempt\":1}");
    assertThat(registry.counter("notifications.retried").count()).isEqualTo(1.0);
  }

  @Test
  void thirdAttemptFailureIsDeadAndDlq() {
    when(query.optional())
        .thenReturn(Optional.of(new SendConsumer.Row(1L, "sms", "flaky-device", "hi", "QUEUED")));
    when(sender.send("sms", "flaky-device", "hi")).thenReturn(ChannelSender.Outcome.TRANSIENT);

    consumer.onEvent("{\"id\":1,\"attempt\":2}");

    verify(jdbc).sql(contains("SET status = 'DEAD'"));
    verify(kafka)
        .send(
            eq(SendConsumer.DLQ_TOPIC),
            eq("1"),
            eq("{\"id\":1,\"attempt\":2,\"reason\":\"attempts exhausted\"}"));
    assertThat(registry.counter("notifications.dead").count()).isEqualTo(1.0);
  }

  @Test
  void permanentFailureIsDeadWithoutRetry() {
    when(query.optional())
        .thenReturn(Optional.of(new SendConsumer.Row(1L, "email", "fail@nowhere", "hi", "QUEUED")));
    when(sender.send("email", "fail@nowhere", "hi")).thenReturn(ChannelSender.Outcome.PERMANENT);

    consumer.onEvent("{\"id\":1,\"attempt\":0}");

    verify(jdbc).sql(contains("SET status = 'DEAD'"));
    verify(kafka).send(eq(SendConsumer.DLQ_TOPIC), eq("1"), contains("permanent"));
    verify(kafka, never()).send(eq(SendConsumer.RETRY_TOPIC), anyString(), anyString());
  }

  @Test
  void lateRetryCannotResurrectTerminalRow() {
    queuedRow("SENT");

    consumer.onEvent("{\"id\":1,\"attempt\":0}");

    verify(jdbc, never()).sql(contains("UPDATE"));
    verify(sender, never()).send(anyString(), anyString(), anyString());
  }

  @Test
  void lostClaimRaceDoesNotSend() {
    queuedRow();
    when(spec.update()).thenReturn(0); // SENDING claim lost to a concurrent terminal update

    consumer.onEvent("{\"id\":1,\"attempt\":0}");

    verify(sender, never()).send(anyString(), anyString(), anyString());
  }

  @Test
  void unknownNotificationIsSkipped() {
    when(query.optional()).thenReturn(Optional.empty());

    consumer.onEvent("{\"id\":1,\"attempt\":0}");

    verify(sender, never()).send(anyString(), anyString(), anyString());
    verify(jdbc, never()).sql(contains("UPDATE"));
  }
}
