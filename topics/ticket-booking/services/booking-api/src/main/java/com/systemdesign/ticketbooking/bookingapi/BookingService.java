package com.systemdesign.ticketbooking.bookingapi;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Seat inventory and booking state machine {@code FREE → HELD → BOOKED} (or back to {@code FREE} on
 * release/expiry). Every transition is a <em>single</em> UPDATE guarded by the current status — the
 * (event_id, seat) row lock taken by that UPDATE is the only concurrency primitive, so there is no
 * SELECT-then-UPDATE window and N racing holds collapse to exactly one winner by construction
 * (decision 001).
 */
@Service
class BookingService {

  /** How long a hold lives before another buyer (or the sweeper) may take the seat. */
  static final String HOLD_TTL_SQL = "now() + interval '5 minutes'";

  private final JdbcClient jdbc;
  private final BookingEventPublisher events;

  BookingService(JdbcClient jdbc, BookingEventPublisher events) {
    this.jdbc = jdbc;
    this.events = events;
  }

  /** Thrown when the state change committed intent but the Kafka audit event could not be sent. */
  static class PublishFailedException extends RuntimeException {
    PublishFailedException(String type) {
      super("booking event " + type + " could not be published");
    }
  }

  // ---- create ---------------------------------------------------------------------------

  /**
   * Inserts the event and its {@code rows*cols} seat rows in one transaction — an event is never
   * visible with a partial seat map. Seats are labelled {@code A1..A<cols>, B1..}.
   */
  @Transactional
  EventCreated createEvent(String name, BigDecimal price, int rows, int cols) {
    long id =
        jdbc.sql("INSERT INTO events (name, price) VALUES (:name, :price) RETURNING id")
            .param("name", name)
            .param("price", price)
            .query(Long.class)
            .single();
    for (int r = 0; r < rows; r++) {
      String rowLabel = String.valueOf((char) ('A' + r));
      for (int c = 1; c <= cols; c++) {
        jdbc.sql("INSERT INTO seats (event_id, seat) VALUES (:eventId, :seat)")
            .param("eventId", id)
            .param("seat", rowLabel + c)
            .update();
      }
    }
    return new EventCreated(id, rows * cols);
  }

  // ---- seat map --------------------------------------------------------------------------

  /**
   * Returns the seat map for an event, or empty when the event does not exist. A seat still {@code
   * HELD} in the row but whose {@code hold_expires_at} has passed is reported as {@code FREE}
   * (filter-on-read): the sweeper rewrites the row lazily, so the read model never shows a stale
   * hold as sold (decision 002).
   */
  Optional<List<SeatView>> seatMap(long eventId) {
    if (!eventExists(eventId)) {
      return Optional.empty();
    }
    OffsetDateTime now = OffsetDateTime.now();
    return Optional.of(
        jdbc.sql(
                "SELECT seat, status, hold_owner, hold_expires_at, booked_by FROM seats"
                    + " WHERE event_id = :eventId ORDER BY seat")
            .param("eventId", eventId)
            .query(
                (rs, rowNum) ->
                    new SeatView(
                        rs.getString("seat"),
                        effectiveStatus(
                            rs.getString("status"),
                            rs.getObject("hold_expires_at", OffsetDateTime.class),
                            now),
                        rs.getString("hold_owner"),
                        rs.getString("booked_by")))
            .list());
  }

  /** The read-side view of a seat: an expired hold reads as FREE even before the sweeper runs. */
  static String effectiveStatus(String status, OffsetDateTime holdExpiresAt, OffsetDateTime now) {
    if ("HELD".equals(status) && holdExpiresAt != null && holdExpiresAt.isBefore(now)) {
      return "FREE";
    }
    return status;
  }

  // ---- hold -------------------------------------------------------------------------------

  /**
   * Single atomic UPDATE: the seat goes to {@code HELD} for {@code owner} only when it is FREE or
   * its previous hold already expired. 1 row updated = the caller won the race; 0 rows = someone
   * else holds it or it is booked. No SELECT decides availability — the UPDATE itself is the claim.
   */
  @Transactional
  HoldOutcome hold(long eventId, String seat, String owner) {
    if (!seatExists(eventId, seat)) {
      return new HoldOutcome(HoldResult.SEAT_NOT_FOUND, null);
    }
    Optional<OffsetDateTime> expiresAt =
        jdbc.sql(
                "UPDATE seats SET status = 'HELD', hold_owner = :owner,"
                    + " hold_expires_at = "
                    + HOLD_TTL_SQL
                    + " WHERE event_id = :eventId AND seat = :seat"
                    + " AND (status = 'FREE' OR (status = 'HELD' AND hold_expires_at < now()))"
                    + " RETURNING hold_expires_at")
            .param("eventId", eventId)
            .param("seat", seat)
            .param("owner", owner)
            .query(OffsetDateTime.class)
            .optional();
    if (expiresAt.isEmpty()) {
      return new HoldOutcome(HoldResult.CONFLICT, null);
    }
    publish("HELD", eventId, seat, owner);
    return new HoldOutcome(HoldResult.HELD, expiresAt.get());
  }

  // ---- confirm ----------------------------------------------------------------------------

  /**
   * Simulated payment + booking inside one transaction. An {@code owner} containing {@code
   * "decline"} fails the payment: the seat is released and a {@code DECLINED} payment row is
   * written. Otherwise the seat flips {@code HELD → BOOKED} — guarded by owner and a live hold, so
   * a confirm after expiry or by the wrong owner updates 0 rows and maps to {@link
   * ConfirmResult#GONE}.
   */
  @Transactional
  ConfirmOutcome confirm(long eventId, String seat, String owner) {
    if (!seatExists(eventId, seat)) {
      return new ConfirmOutcome(ConfirmResult.SEAT_NOT_FOUND);
    }
    if (owner.toLowerCase(Locale.ROOT).contains("decline")) {
      jdbc.sql(
              "UPDATE seats SET status = 'FREE', hold_owner = null, hold_expires_at = null"
                  + " WHERE event_id = :eventId AND seat = :seat"
                  + " AND status = 'HELD' AND hold_owner = :owner")
          .param("eventId", eventId)
          .param("seat", seat)
          .param("owner", owner)
          .update();
      recordPayment(eventId, seat, owner, "DECLINED");
      publish("DECLINED", eventId, seat, owner);
      return new ConfirmOutcome(ConfirmResult.DECLINED);
    }
    int booked =
        jdbc.sql(
                "UPDATE seats SET status = 'BOOKED', booked_by = :owner, hold_expires_at = null"
                    + " WHERE event_id = :eventId AND seat = :seat"
                    + " AND status = 'HELD' AND hold_owner = :owner"
                    + " AND hold_expires_at > now()")
            .param("eventId", eventId)
            .param("seat", seat)
            .param("owner", owner)
            .update();
    if (booked == 0) {
      // Hold expired, seat is held by someone else, or already booked — the client must re-hold.
      publish("EXPIRED_ON_CONFIRM", eventId, seat, owner);
      return new ConfirmOutcome(ConfirmResult.GONE);
    }
    recordPayment(eventId, seat, owner, "PAID");
    publish("CONFIRMED", eventId, seat, owner);
    return new ConfirmOutcome(ConfirmResult.CONFIRMED);
  }

  // ---- release -----------------------------------------------------------------------------

  /**
   * Explicit release by the hold owner. The UPDATE is guarded by {@code hold_owner}, so a client
   * can never free a seat somebody else holds — including a seat re-held after this owner's hold
   * expired.
   */
  @Transactional
  boolean release(long eventId, String seat, String owner) {
    int released =
        jdbc.sql(
                "UPDATE seats SET status = 'FREE', hold_owner = null, hold_expires_at = null"
                    + " WHERE event_id = :eventId AND seat = :seat"
                    + " AND status = 'HELD' AND hold_owner = :owner")
            .param("eventId", eventId)
            .param("seat", seat)
            .param("owner", owner)
            .update();
    if (released == 0) {
      return false;
    }
    publish("RELEASED", eventId, seat, owner);
    return true;
  }

  // ---- internals ----------------------------------------------------------------------------

  private boolean eventExists(long eventId) {
    return jdbc.sql("SELECT 1 FROM events WHERE id = :id")
        .param("id", eventId)
        .query(Integer.class)
        .optional()
        .isPresent();
  }

  private boolean seatExists(long eventId, String seat) {
    return jdbc.sql("SELECT 1 FROM seats WHERE event_id = :eventId AND seat = :seat")
        .param("eventId", eventId)
        .param("seat", seat)
        .query(Integer.class)
        .optional()
        .isPresent();
  }

  private void recordPayment(long eventId, String seat, String owner, String status) {
    BigDecimal price =
        jdbc.sql("SELECT price FROM events WHERE id = :id")
            .param("id", eventId)
            .query(BigDecimal.class)
            .single();
    jdbc.sql(
            "INSERT INTO payments (event_id, seat, owner, amount, status)"
                + " VALUES (:eventId, :seat, :owner, :amount, :status)")
        .param("eventId", eventId)
        .param("seat", seat)
        .param("owner", owner)
        .param("amount", price)
        .param("status", status)
        .update();
  }

  private void publish(String type, long eventId, String seat, String owner) {
    if (!events.publish(type, eventId, seat, owner)) {
      // Roll back the state change: a hold/confirm without its audit event never happened.
      throw new PublishFailedException(type);
    }
  }

  record EventCreated(long id, int seatCount) {}

  record SeatView(String seat, String status, String holdOwner, String bookedBy) {}

  enum HoldResult {
    HELD,
    CONFLICT,
    SEAT_NOT_FOUND
  }

  record HoldOutcome(HoldResult result, OffsetDateTime expiresAt) {}

  enum ConfirmResult {
    CONFIRMED,
    DECLINED,
    GONE,
    SEAT_NOT_FOUND
  }

  record ConfirmOutcome(ConfirmResult result) {}
}
