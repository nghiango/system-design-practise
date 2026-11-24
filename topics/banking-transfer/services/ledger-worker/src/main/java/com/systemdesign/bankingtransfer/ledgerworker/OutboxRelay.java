package com.systemdesign.bankingtransfer.ledgerworker;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Transactional-outbox relay (decision 001). Every second it claims up to 100 unpublished rows
 * {@code FOR UPDATE SKIP LOCKED} — multiple worker replicas can relay concurrently without
 * double-claiming the same row — publishes each payload to {@code banking-transfer.ledger.entries}
 * keyed by transfer id (per-transfer ordering), then marks the rows published in the same
 * transaction that held the locks.
 *
 * <p>Delivery is <em>at-least-once</em>: a crash after {@code kafka.send} but before the commit
 * re-publishes the row on the next tick. That is safe because the consumer dedupes on {@code
 * (transfer_id, account_id, entry)} — and it is the deliberate trade: never lose an event, dedupe
 * downstream, versus best-effort publish which silently drops events (decision 002).
 */
@Component
class OutboxRelay {

  static final String TOPIC = "banking-transfer.ledger.entries";

  private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

  private final JdbcClient jdbc;
  private final KafkaTemplate<String, String> kafka;
  private final ObjectMapper mapper;
  private final Counter publishedCounter;

  OutboxRelay(
      JdbcClient jdbc,
      KafkaTemplate<String, String> kafka,
      ObjectMapper mapper,
      MeterRegistry registry) {
    this.jdbc = jdbc;
    this.kafka = kafka;
    this.mapper = mapper;
    this.publishedCounter = registry.counter("outbox.published");
  }

  @Scheduled(fixedDelayString = "${sd.relay.delay-ms:1000}")
  @Transactional
  void relay() {
    var rows =
        jdbc.sql(
                "SELECT id, payload FROM outbox WHERE NOT published"
                    + " ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED")
            .query((rs, rowNum) -> new OutboxRow(rs.getLong("id"), rs.getString("payload")))
            .list();
    for (OutboxRow row : rows) {
      try {
        kafka.send(TOPIC, keyOf(row.payload()), row.payload()).get();
      } catch (Exception e) {
        // Roll back: none of this batch is marked published, the next tick retries all of it.
        throw new RelayException("publish failed for outbox row " + row.id(), e);
      }
      jdbc.sql("UPDATE outbox SET published = true WHERE id = :id").param("id", row.id()).update();
      publishedCounter.increment();
    }
  }

  /** The Kafka key is the transfer id inside the payload, so one transfer's events stay ordered. */
  private String keyOf(String payload) {
    try {
      JsonNode node = mapper.readTree(payload);
      return String.valueOf(node.path("transferId").asLong(0));
    } catch (Exception e) {
      log.warn("outbox payload has no transferId, using row key: {}", payload);
      return "0";
    }
  }

  static class RelayException extends RuntimeException {
    RelayException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  record OutboxRow(long id, String payload) {}
}
