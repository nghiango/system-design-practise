package com.systemdesign.ticketbooking.bookingapi;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/events")
class BookingController {

  private static final int MAX_ROWS = 26; // seat labels are single letters A..Z
  private static final int MAX_COLS = 500;

  private final BookingService bookings;
  private final Counter heldCounter;
  private final Counter bookedCounter;
  private final Counter conflictCounter;

  BookingController(BookingService bookings, MeterRegistry registry) {
    this.bookings = bookings;
    this.heldCounter = registry.counter("seats.held");
    this.bookedCounter = registry.counter("seats.booked");
    this.conflictCounter = registry.counter("seats.conflict");
  }

  @PostMapping
  ResponseEntity<?> createEvent(@RequestBody CreateEventRequest request) {
    if (request == null
        || request.name() == null
        || request.name().isBlank()
        || request.price() == null
        || request.price().signum() < 0
        || request.rows() < 1
        || request.rows() > MAX_ROWS
        || request.cols() < 1
        || request.cols() > MAX_COLS) {
      return ResponseEntity.badRequest()
          .body(
              new ErrorResponse(
                  "name, non-negative price, rows 1.."
                      + MAX_ROWS
                      + " and cols 1.."
                      + MAX_COLS
                      + " are required"));
    }
    BookingService.EventCreated created =
        bookings.createEvent(
            request.name().trim(), request.price(), request.rows(), request.cols());
    return ResponseEntity.status(HttpStatus.CREATED)
        .body(new EventResponse(created.id(), created.seatCount()));
  }

  @GetMapping("/{id}/seats")
  ResponseEntity<List<SeatResponse>> seats(@PathVariable long id) {
    return bookings
        .seatMap(id)
        .map(
            seats ->
                seats.stream()
                    .map(s -> new SeatResponse(s.seat(), s.status(), s.holdOwner(), s.bookedBy()))
                    .toList())
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  @PostMapping("/{id}/hold")
  ResponseEntity<?> hold(@PathVariable long id, @RequestBody HoldRequest request) {
    if (request == null
        || request.seat() == null
        || request.seat().isBlank()
        || request.owner() == null
        || request.owner().isBlank()) {
      return ResponseEntity.badRequest().body(new ErrorResponse("seat and owner are required"));
    }
    BookingService.HoldOutcome outcome = bookings.hold(id, request.seat(), request.owner());
    return switch (outcome.result()) {
      case HELD -> {
        heldCounter.increment();
        yield ResponseEntity.ok(new HoldResponse(request.seat(), outcome.expiresAt()));
      }
      case CONFLICT -> {
        conflictCounter.increment();
        yield ResponseEntity.status(HttpStatus.CONFLICT)
            .body(new ErrorResponse("seat is held or booked"));
      }
      case SEAT_NOT_FOUND ->
          ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ErrorResponse("unknown seat"));
    };
  }

  @PostMapping("/{id}/confirm")
  ResponseEntity<?> confirm(@PathVariable long id, @RequestBody ConfirmRequest request) {
    if (request == null
        || request.seat() == null
        || request.seat().isBlank()
        || request.owner() == null
        || request.owner().isBlank()) {
      return ResponseEntity.badRequest().body(new ErrorResponse("seat and owner are required"));
    }
    BookingService.ConfirmOutcome outcome = bookings.confirm(id, request.seat(), request.owner());
    return switch (outcome.result()) {
      case CONFIRMED -> {
        bookedCounter.increment();
        yield ResponseEntity.ok(new ConfirmResponse(request.seat(), "BOOKED"));
      }
      case DECLINED ->
          ResponseEntity.status(HttpStatus.PAYMENT_REQUIRED)
              .body(new ErrorResponse("payment declined, seat released"));
      case GONE -> {
        conflictCounter.increment();
        yield ResponseEntity.status(HttpStatus.GONE)
            .body(new ErrorResponse("hold expired or owned by someone else"));
      }
      case SEAT_NOT_FOUND ->
          ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ErrorResponse("unknown seat"));
    };
  }

  @DeleteMapping("/{id}/hold")
  ResponseEntity<?> release(@PathVariable long id, @RequestBody HoldRequest request) {
    if (request == null
        || request.seat() == null
        || request.seat().isBlank()
        || request.owner() == null
        || request.owner().isBlank()) {
      return ResponseEntity.badRequest().body(new ErrorResponse("seat and owner are required"));
    }
    if (!bookings.release(id, request.seat(), request.owner())) {
      conflictCounter.increment();
      return ResponseEntity.status(HttpStatus.CONFLICT)
          .body(new ErrorResponse("seat is not held by this owner"));
    }
    return ResponseEntity.noContent().build();
  }

  @ExceptionHandler(BookingService.PublishFailedException.class)
  ResponseEntity<ErrorResponse> publishFailed(BookingService.PublishFailedException e) {
    // The transaction rolled back with the state change; a retry replays the whole operation.
    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
        .body(new ErrorResponse("booking event queue unavailable, retry the request"));
  }

  record CreateEventRequest(String name, BigDecimal price, int rows, int cols) {}

  record EventResponse(long id, int seatCount) {}

  record SeatResponse(String seat, String status, String holdOwner, String bookedBy) {}

  record HoldRequest(String seat, String owner) {}

  record ConfirmRequest(String seat, String owner) {}

  record HoldResponse(String seat, OffsetDateTime expiresAt) {}

  record ConfirmResponse(String seat, String status) {}

  record ErrorResponse(String error) {}
}
