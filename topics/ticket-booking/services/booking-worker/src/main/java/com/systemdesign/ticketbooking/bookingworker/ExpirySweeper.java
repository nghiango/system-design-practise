package com.systemdesign.ticketbooking.bookingworker;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Lazily frees seats whose hold expired. A single {@code UPDATE ... WHERE status='HELD' AND
 * hold_expires_at < now() RETURNING ...} reclaims every dead hold in one statement — the row lock
 * taken by the UPDATE makes the reclaim atomic against a racing hold/confirm on the same seat, and
 * {@code RETURNING} reports exactly which seats were freed so each gets an {@code EXPIRED} audit
 * event (decision 002).
 *
 * <p>The API never depends on the sweeper for correctness: the hold UPDATE and the seat map both
 * treat an expired hold as free on their own. The sweeper only makes the stored row match reality
 * and produces the audit event.
 */
@Component
class ExpirySweeper {

  static final String TOPIC = "ticket-booking.booking.events";

  private static final Logger log = LoggerFactory.getLogger(ExpirySweeper.class);

  private final JdbcClient jdbc;
  private final KafkaTemplate<String, String> kafka;
  private final Counter expiredCounter;

  ExpirySweeper(JdbcClient jdbc, KafkaTemplate<String, String> kafka, MeterRegistry registry) {
    this.jdbc = jdbc;
    this.kafka = kafka;
    this.expiredCounter = registry.counter("seats.expired");
  }

  @Scheduled(fixedDelayString = "${sd.sweeper.delay-ms:10000}")
  void sweep() {
    var released =
        jdbc.sql(
                "UPDATE seats SET status = 'FREE', hold_owner = null, hold_expires_at = null"
                    + " WHERE status = 'HELD' AND hold_expires_at < now()"
                    + " RETURNING event_id, seat, hold_owner")
            .query(
                (rs, rowNum) ->
                    new ReleasedSeat(
                        rs.getLong("event_id"), rs.getString("seat"), rs.getString("hold_owner")))
            .list();
    for (ReleasedSeat seat : released) {
      expiredCounter.increment();
      try {
        kafka.send(
            TOPIC,
            seat.eventId() + ":" + seat.seat(),
            "{\"type\":\"EXPIRED\",\"eventId\":"
                + seat.eventId()
                + ",\"seat\":\""
                + seat.seat()
                + "\",\"owner\":\""
                + seat.owner()
                + "\"}");
      } catch (Exception e) {
        // Best-effort: the freed row is the durable fact; a lost EXPIRED only thins the audit.
        log.warn("expired event publish failed for {}: {}", seat, e.toString());
      }
    }
  }

  record ReleasedSeat(long eventId, String seat, String owner) {}
}
