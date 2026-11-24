package com.systemdesign.notification.senderworker;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Sends notifications from {@code notification.requested} and {@code notification.retry} through a
 * strict status machine: {@code QUEUED → SENDING → SENT}, transient failures requeue ({@code
 * SENDING → QUEUED}, attempts+1) and permanent failure or exhausted attempts go {@code → DEAD} plus
 * a {@code notification.dlq} event.
 *
 * <p>Attempt accounting: an <em>attempt</em> is a consumed rate-limit token followed by a provider
 * call. Being throttled is not a failure — a throttled event is republished to {@code
 * notification.retry} with the same attempt number and the row stays QUEUED (decision 002). {@code
 * MAX_ATTEMPTS} bounds total provider calls at 3; the third failure lands on the DLQ.
 *
 * <p>Every UPDATE is guarded by the current status, so a late retry delivery can never resurrect a
 * SENT or DEAD row — at-least-once Kafka delivery is made safe by the state machine, not by
 * "exactly-once" claims.
 */
@Component
class SendConsumer {

  static final String REQUESTED_TOPIC = "notification.requested";
  static final String RETRY_TOPIC = "notification.retry";
  static final String DLQ_TOPIC = "notification.dlq";
  static final int MAX_ATTEMPTS = 3;

  private static final Logger log = LoggerFactory.getLogger(SendConsumer.class);

  private final JdbcClient jdbc;
  private final KafkaTemplate<String, String> kafka;
  private final RateLimiter limiter;
  private final ChannelSender sender;
  private final ObjectMapper mapper;
  private final Counter sentCounter;
  private final Counter deadCounter;
  private final Counter retriedCounter;

  SendConsumer(
      JdbcClient jdbc,
      KafkaTemplate<String, String> kafka,
      RateLimiter limiter,
      ChannelSender sender,
      ObjectMapper mapper,
      MeterRegistry registry) {
    this.jdbc = jdbc;
    this.kafka = kafka;
    this.limiter = limiter;
    this.sender = sender;
    this.mapper = mapper;
    this.sentCounter = registry.counter("notifications.sent");
    this.deadCounter = registry.counter("notifications.dead");
    this.retriedCounter = registry.counter("notifications.retried");
  }

  @KafkaListener(
      topics = {REQUESTED_TOPIC, RETRY_TOPIC},
      groupId = "${SD_KAFKA_GROUP_PREFIX:}sender")
  void onEvent(String message) {
    SendEvent event = extract(message);
    if (event == null) {
      return;
    }
    Optional<Row> found = findRow(event.id());
    if (found.isEmpty() || isTerminal(found.get().status())) {
      // Unknown id or already finished — a late retry must not resurrect it.
      return;
    }
    Row row = found.get();

    if (!limiter.tryAcquire(row.channel())) {
      // Throttled: republish the same attempt. Throttling never burns the attempt budget.
      republish(RETRY_TOPIC, event);
      return;
    }

    int claimed =
        jdbc.sql(
                "UPDATE notifications SET status = 'SENDING', updated_at = now()"
                    + " WHERE id = :id AND status IN ('QUEUED', 'SENDING')")
            .param("id", event.id())
            .update();
    if (claimed == 0) {
      return; // raced to a terminal state between the read and the claim
    }

    ChannelSender.Outcome outcome = sender.send(row.channel(), row.target(), row.body());
    switch (outcome) {
      case SUCCESS -> {
        jdbc.sql(
                "UPDATE notifications SET status = 'SENT', updated_at = now()"
                    + " WHERE id = :id AND status = 'SENDING'")
            .param("id", event.id())
            .update();
        sentCounter.increment();
      }
      case PERMANENT -> dead(event, "permanent provider error");
      case TRANSIENT -> {
        if (event.attempt() + 1 < MAX_ATTEMPTS) {
          jdbc.sql(
                  "UPDATE notifications SET status = 'QUEUED', attempts = :attempts,"
                      + " updated_at = now() WHERE id = :id AND status = 'SENDING'")
              .param("attempts", event.attempt() + 1)
              .param("id", event.id())
              .update();
          republish(RETRY_TOPIC, new SendEvent(event.id(), event.attempt() + 1));
          retriedCounter.increment();
        } else {
          dead(event, "attempts exhausted");
        }
      }
    }
  }

  /** Marks DEAD (guarded: terminal rows never move backwards) and emits the DLQ event. */
  private void dead(SendEvent event, String reason) {
    int updated =
        jdbc.sql(
                "UPDATE notifications SET status = 'DEAD', updated_at = now()"
                    + " WHERE id = :id AND status IN ('QUEUED', 'SENDING')")
            .param("id", event.id())
            .update();
    if (updated > 0) {
      deadCounter.increment();
      try {
        kafka.send(
            DLQ_TOPIC,
            Long.toString(event.id()),
            "{\"id\":"
                + event.id()
                + ",\"attempt\":"
                + event.attempt()
                + ",\"reason\":\""
                + reason
                + "\"}");
      } catch (Exception e) {
        // Best-effort: the DEAD row is the durable record; the DLQ event is for operators.
        log.warn("dlq publish failed for {}: {}", event.id(), e.toString());
      }
    }
  }

  private void republish(String topic, SendEvent event) {
    try {
      kafka
          .send(
              topic,
              Long.toString(event.id()),
              "{\"id\":" + event.id() + ",\"attempt\":" + event.attempt() + "}")
          .get();
    } catch (Exception e) {
      // Row stays QUEUED: a crashed publish is safe (the send never claimed) but the notification
      // stalls until something republishes — same stall class as a broker outage, logged loudly.
      log.error("requeue publish failed for {}: {}", event.id(), e.toString());
    }
  }

  private Optional<Row> findRow(long id) {
    return jdbc.sql("SELECT id, channel, target, body, status FROM notifications WHERE id = :id")
        .param("id", id)
        .query(
            (rs, rowNum) ->
                new Row(
                    rs.getLong("id"),
                    rs.getString("channel"),
                    rs.getString("target"),
                    rs.getString("body"),
                    rs.getString("status")))
        .optional();
  }

  private boolean isTerminal(String status) {
    return "SENT".equals(status) || "DEAD".equals(status);
  }

  SendEvent extract(String message) {
    try {
      JsonNode node = mapper.readTree(message);
      JsonNode id = node.get("id");
      JsonNode attempt = node.get("attempt");
      if (id == null || !id.isNumber() || attempt == null || !attempt.isNumber()) {
        return null;
      }
      return new SendEvent(id.asLong(), attempt.asInt());
    } catch (Exception e) {
      return null;
    }
  }

  record SendEvent(long id, int attempt) {}

  record Row(long id, String channel, String target, String body, String status) {}
}
