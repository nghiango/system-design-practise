package com.systemdesign.ticketbooking.bookingapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes booking lifecycle events ({@code HELD}, {@code CONFIRMED}, {@code DECLINED}, {@code
 * RELEASED}, {@code EXPIRED_ON_CONFIRM}) to {@code ticket-booking.booking.events}, keyed by seat so
 * per-seat ordering is preserved for the audit consumer.
 *
 * <p>The publish is synchronous: a lost {@code CONFIRMED} event means a paid seat never reaches the
 * audit trail, so a publish failure turns the confirm/hold response into a 503 rather than
 * pretending the booking succeeded.
 */
@Component
class BookingEventPublisher {

  static final String TOPIC = "ticket-booking.booking.events";

  private static final Logger log = LoggerFactory.getLogger(BookingEventPublisher.class);

  private final KafkaTemplate<String, String> kafka;

  BookingEventPublisher(KafkaTemplate<String, String> kafka) {
    this.kafka = kafka;
  }

  /** Returns {@code true} when the event was handed to the broker. */
  boolean publish(String type, long eventId, String seat, String owner) {
    String payload =
        "{\"type\":\""
            + type
            + "\",\"eventId\":"
            + eventId
            + ",\"seat\":\""
            + seat
            + "\",\"owner\":\""
            + owner
            + "\"}";
    try {
      kafka.send(TOPIC, eventId + ":" + seat, payload).get();
      return true;
    } catch (Exception e) {
      log.warn("booking event publish failed for {}: {}", payload, e.toString());
      return false;
    }
  }
}
