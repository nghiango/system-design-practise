package com.systemdesign.ticketbooking.bookingworker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Appends every event on {@code ticket-booking.booking.events} to {@code booking_history}, giving
 * the seat lifecycle an append-only audit trail served by {@link HistoryController}.
 *
 * <p>Delivery is at-least-once: a redelivery can append a duplicate row. History is a diagnosis
 * trail, not a balance — duplicates are honest evidence of redelivery, so no dedupe key is
 * fabricated (documented in the topic README).
 */
@Component
class AuditConsumer {

  static final String TOPIC = "ticket-booking.booking.events";

  private static final Logger log = LoggerFactory.getLogger(AuditConsumer.class);

  private final JdbcClient jdbc;
  private final ObjectMapper mapper;

  AuditConsumer(JdbcClient jdbc, ObjectMapper mapper) {
    this.jdbc = jdbc;
    this.mapper = mapper;
  }

  @KafkaListener(topics = TOPIC, groupId = "${SD_KAFKA_GROUP_PREFIX:}audit")
  void onEvent(String message) {
    JsonNode event;
    try {
      event = mapper.readTree(message);
    } catch (Exception e) {
      // A malformed event has no valid target row — drop it rather than wedging the partition.
      log.warn("dropping unparseable booking event: {}", message);
      return;
    }
    jdbc.sql(
            "INSERT INTO booking_history (event_type, event_id, seat, owner)"
                + " VALUES (:type, :eventId, :seat, :owner)")
        .param("type", event.path("type").asText("UNKNOWN"))
        .param("eventId", event.path("eventId").asLong(0))
        .param("seat", event.path("seat").isNull() ? null : event.path("seat").asText())
        .param("owner", event.path("owner").isNull() ? null : event.path("owner").asText())
        .update();
  }
}
