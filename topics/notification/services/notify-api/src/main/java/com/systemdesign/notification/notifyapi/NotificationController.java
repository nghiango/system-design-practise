package com.systemdesign.notification.notifyapi;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Set;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/notifications")
class NotificationController {

  private static final Set<String> CHANNELS = Set.of("email", "sms");

  private final NotificationService notifications;
  private final NotificationEventPublisher events;
  private final Counter submittedCounter;
  private final Counter deduplicatedCounter;

  NotificationController(
      NotificationService notifications,
      NotificationEventPublisher events,
      MeterRegistry registry) {
    this.notifications = notifications;
    this.events = events;
    this.submittedCounter = registry.counter("notifications.submitted");
    this.deduplicatedCounter = registry.counter("notifications.deduplicated");
  }

  @PostMapping
  ResponseEntity<?> submit(@RequestBody SubmitRequest request) {
    if (request == null
        || request.idempotencyKey() == null
        || request.idempotencyKey().isBlank()
        || request.channel() == null
        || request.target() == null
        || request.target().isBlank()
        || request.body() == null) {
      return ResponseEntity.badRequest()
          .body(new ErrorResponse("idempotencyKey, channel, target and body are required"));
    }
    if (!CHANNELS.contains(request.channel())) {
      return ResponseEntity.badRequest()
          .body(new ErrorResponse("channel must be one of " + CHANNELS));
    }

    NotificationService.Submission submission =
        notifications.submit(
            request.idempotencyKey(), request.channel(), request.target(), request.body());
    if (!submission.created()) {
      // Idempotent retry: the row (and its send event) already exist — never queue a second send.
      deduplicatedCounter.increment();
      return ResponseEntity.ok(new SubmitResponse(submission.id(), submission.status()));
    }
    if (!events.publish(submission.id())) {
      // The row is persisted QUEUED; a reaper could republish. We report failure so the client
      // retries with the same idempotency key, which deduplicates against the stored row.
      return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
          .body(new ErrorResponse("send queue unavailable, retry with the same idempotencyKey"));
    }
    submittedCounter.increment();
    return ResponseEntity.status(HttpStatus.CREATED)
        .body(new SubmitResponse(submission.id(), submission.status()));
  }

  @GetMapping("/{id}")
  ResponseEntity<NotificationResponse> get(@PathVariable long id) {
    return notifications
        .find(id)
        .map(n -> new NotificationResponse(n.id(), n.channel(), n.status(), n.attempts()))
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  record SubmitRequest(String idempotencyKey, String channel, String target, String body) {}

  record SubmitResponse(long id, String status) {}

  record NotificationResponse(long id, String channel, String status, int attempts) {}

  record ErrorResponse(String error) {}
}
