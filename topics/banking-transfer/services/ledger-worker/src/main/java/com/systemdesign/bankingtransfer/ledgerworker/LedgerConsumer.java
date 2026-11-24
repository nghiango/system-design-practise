package com.systemdesign.bankingtransfer.ledgerworker;

import java.math.BigDecimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Projects every {@code transfer.completed} event into {@code ledger_audit}. The event payload
 * carries both ledger entries, and the projection's {@code UNIQUE (transfer_id, account_id, entry)}
 * constraint is the dedupe: at-least-once delivery (outbox relay republish after a crash, Kafka
 * redelivery) simply re-runs {@code INSERT ... ON CONFLICT DO NOTHING}, which is a no-op. Malformed
 * events are dropped rather than wedging the partition.
 */
@Component
class LedgerConsumer {

  private static final Logger log = LoggerFactory.getLogger(LedgerConsumer.class);

  private final JdbcClient jdbc;
  private final ObjectMapper mapper;

  LedgerConsumer(JdbcClient jdbc, ObjectMapper mapper) {
    this.jdbc = jdbc;
    this.mapper = mapper;
  }

  @KafkaListener(topics = OutboxRelay.TOPIC, groupId = "${SD_KAFKA_GROUP_PREFIX:}projection")
  @Transactional
  void onEvent(String message) {
    JsonNode event;
    try {
      event = mapper.readTree(message);
    } catch (Exception e) {
      log.warn("dropping unparseable ledger event: {}", message);
      return;
    }
    JsonNode entries = event.path("entries");
    if (!event.path("transferId").isNumber() || !entries.isArray()) {
      // transfer.rejected (and anything else without entries) has nothing to project.
      return;
    }
    long transferId = event.path("transferId").asLong();
    for (JsonNode entry : entries) {
      jdbc.sql(
              "INSERT INTO ledger_audit (transfer_id, account_id, entry, amount, balance_after)"
                  + " VALUES (:transferId, :accountId, :entry, :amount, :balanceAfter)"
                  + " ON CONFLICT (transfer_id, account_id, entry) DO NOTHING")
          .param("transferId", transferId)
          .param("accountId", entry.path("accountId").asLong())
          .param("entry", entry.path("entry").asText())
          .param("amount", new BigDecimal(entry.path("amount").asText("0")))
          .param("balanceAfter", new BigDecimal(entry.path("balanceAfter").asText("0")))
          .update();
    }
  }
}
