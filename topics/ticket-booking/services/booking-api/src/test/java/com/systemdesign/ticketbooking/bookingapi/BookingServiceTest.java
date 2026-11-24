package com.systemdesign.ticketbooking.bookingapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.simple.JdbcClient;

class BookingServiceTest {

  private JdbcClient jdbc;
  private BookingEventPublisher events;
  private BookingService service;

  @BeforeEach
  void setUp() {
    jdbc = mock(JdbcClient.class, Mockito.RETURNS_DEEP_STUBS);
    events = mock(BookingEventPublisher.class);
    when(events.publish(anyString(), anyLong(), anyString(), anyString())).thenReturn(true);
    service = new BookingService(jdbc, events);
  }

  private void seatExists(boolean exists) {
    when(jdbc.sql(contains("SELECT 1 FROM seats"))
            .param(anyString(), any())
            .param(anyString(), any())
            .query(Integer.class)
            .optional())
        .thenReturn(exists ? Optional.of(1) : Optional.empty());
  }

  // ---- hold ------------------------------------------------------------------------------

  @Test
  void holdWinsWhenAtomicUpdateClaimsTheRow() {
    seatExists(true);
    OffsetDateTime expires = OffsetDateTime.now().plusMinutes(5);
    when(jdbc.sql(contains("SET status = 'HELD'"))
            .param(anyString(), any())
            .param(anyString(), any())
            .param(anyString(), any())
            .query(OffsetDateTime.class)
            .optional())
        .thenReturn(Optional.of(expires));

    BookingService.HoldOutcome outcome = service.hold(1L, "A1", "alice");

    assertThat(outcome.result()).isEqualTo(BookingService.HoldResult.HELD);
    assertThat(outcome.expiresAt()).isEqualTo(expires);
    verify(events).publish("HELD", 1L, "A1", "alice");
  }

  @Test
  void holdLosesWhenAtomicUpdateClaimsNoRow() {
    seatExists(true);
    when(jdbc.sql(contains("SET status = 'HELD'"))
            .param(anyString(), any())
            .param(anyString(), any())
            .param(anyString(), any())
            .query(OffsetDateTime.class)
            .optional())
        .thenReturn(Optional.empty());

    BookingService.HoldOutcome outcome = service.hold(1L, "A1", "bob");

    assertThat(outcome.result()).isEqualTo(BookingService.HoldResult.CONFLICT);
    verify(events, never()).publish(anyString(), anyLong(), anyString(), anyString());
  }

  @Test
  void holdRejectsUnknownSeat() {
    seatExists(false);

    BookingService.HoldOutcome outcome = service.hold(1L, "Z9", "alice");

    assertThat(outcome.result()).isEqualTo(BookingService.HoldResult.SEAT_NOT_FOUND);
    verify(jdbc, never()).sql(contains("SET status = 'HELD'"));
  }

  @Test
  void holdClaimIsASingleGuardedUpdateNotSelectThenUpdate() {
    seatExists(true);
    when(jdbc.sql(contains("SET status = 'HELD'"))
            .param(anyString(), any())
            .param(anyString(), any())
            .param(anyString(), any())
            .query(OffsetDateTime.class)
            .optional())
        .thenReturn(Optional.of(OffsetDateTime.now()));

    service.hold(1L, "A1", "alice");

    // The availability condition lives inside the UPDATE's WHERE clause — the row lock taken by
    // the UPDATE is the claim, so racing holders cannot both win.
    verify(jdbc)
        .sql(
            contains(
                "WHERE event_id = :eventId AND seat = :seat"
                    + " AND (status = 'FREE' OR (status = 'HELD' AND hold_expires_at < now()))"));
  }

  // ---- confirm ----------------------------------------------------------------------------

  @Test
  void confirmBooksSeatWhenHoldIsLiveAndOwned() {
    seatExists(true);
    when(jdbc.sql(contains("SET status = 'BOOKED'"))
            .param(anyString(), any())
            .param(anyString(), any())
            .param(anyString(), any())
            .update())
        .thenReturn(1);

    BookingService.ConfirmOutcome outcome = service.confirm(1L, "A1", "alice");

    assertThat(outcome.result()).isEqualTo(BookingService.ConfirmResult.CONFIRMED);
    verify(jdbc).sql(contains("INSERT INTO payments"));
    verify(events).publish("CONFIRMED", 1L, "A1", "alice");
  }

  @Test
  void confirmAfterExpiryOrWrongOwnerIsGone() {
    seatExists(true);
    when(jdbc.sql(contains("SET status = 'BOOKED'"))
            .param(anyString(), any())
            .param(anyString(), any())
            .param(anyString(), any())
            .update())
        .thenReturn(0);

    BookingService.ConfirmOutcome outcome = service.confirm(1L, "A1", "mallory");

    assertThat(outcome.result()).isEqualTo(BookingService.ConfirmResult.GONE);
    verify(events).publish("EXPIRED_ON_CONFIRM", 1L, "A1", "mallory");
    verify(jdbc, never()).sql(contains("INSERT INTO payments"));
  }

  @Test
  void confirmDeclineReleasesSeatAndRecordsDeclinedPayment() {
    seatExists(true);
    when(jdbc.sql(contains("SET status = 'FREE'"))
            .param(anyString(), any())
            .param(anyString(), any())
            .param(anyString(), any())
            .update())
        .thenReturn(1);

    BookingService.ConfirmOutcome outcome = service.confirm(1L, "A1", "decline-alice");

    assertThat(outcome.result()).isEqualTo(BookingService.ConfirmResult.DECLINED);
    verify(jdbc).sql(contains("'FREE'")); // seat released
    verify(jdbc).sql(contains("INSERT INTO payments")); // DECLINED payment row
    verify(jdbc, never()).sql(contains("SET status = 'BOOKED'"));
    verify(events).publish("DECLINED", 1L, "A1", "decline-alice");
  }

  // ---- release -----------------------------------------------------------------------------

  @Test
  void releaseOnlySucceedsForTheHoldOwner() {
    when(jdbc.sql(contains("hold_owner = :owner"))
            .param(anyString(), any())
            .param(anyString(), any())
            .param(anyString(), any())
            .update())
        .thenReturn(0);

    assertThat(service.release(1L, "A1", "mallory")).isFalse();
    verify(events, never()).publish(anyString(), anyLong(), anyString(), anyString());
  }

  // ---- seat map ----------------------------------------------------------------------------

  @Test
  void expiredHoldReadsAsFreeBeforeTheSweeperRuns() {
    OffsetDateTime now = OffsetDateTime.now();
    assertThat(BookingService.effectiveStatus("HELD", now.minusSeconds(1), now)).isEqualTo("FREE");
    assertThat(BookingService.effectiveStatus("HELD", now.plusSeconds(1), now)).isEqualTo("HELD");
    assertThat(BookingService.effectiveStatus("FREE", null, now)).isEqualTo("FREE");
    assertThat(BookingService.effectiveStatus("BOOKED", null, now)).isEqualTo("BOOKED");
  }
}
