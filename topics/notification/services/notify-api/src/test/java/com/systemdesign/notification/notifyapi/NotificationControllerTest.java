package com.systemdesign.notification.notifyapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class NotificationControllerTest {

  private NotificationService service;
  private NotificationEventPublisher events;
  private SimpleMeterRegistry registry;
  private NotificationController controller;

  @BeforeEach
  void setUp() {
    service = mock(NotificationService.class);
    events = mock(NotificationEventPublisher.class);
    registry = new SimpleMeterRegistry();
    controller = new NotificationController(service, events, registry);
  }

  private NotificationController.SubmitRequest request(String key, String channel) {
    return new NotificationController.SubmitRequest(key, channel, "user@example.com", "hi");
  }

  @Test
  void submitInsertsPublishesAndReturns201() {
    when(service.submit("k1", "email", "user@example.com", "hi"))
        .thenReturn(new NotificationService.Submission(42L, "QUEUED", true));
    when(events.publish(42L)).thenReturn(true);

    ResponseEntity<?> res = controller.submit(request("k1", "email"));

    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(res.getBody()).isEqualTo(new NotificationController.SubmitResponse(42L, "QUEUED"));
    verify(events).publish(42L);
    assertThat(registry.counter("notifications.submitted").count()).isEqualTo(1.0);
  }

  @Test
  void duplicateSubmitReturnsExistingIdWithoutPublishing() {
    when(service.submit("k1", "email", "user@example.com", "hi"))
        .thenReturn(new NotificationService.Submission(42L, "SENT", false));

    ResponseEntity<?> res = controller.submit(request("k1", "email"));

    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(res.getBody()).isEqualTo(new NotificationController.SubmitResponse(42L, "SENT"));
    verify(events, never()).publish(anyLong());
    assertThat(registry.counter("notifications.deduplicated").count()).isEqualTo(1.0);
  }

  @Test
  void unknownChannelIsRejected() {
    for (String bad : new String[] {"push", "slack", "", "EMAIL"}) {
      ResponseEntity<?> res = controller.submit(request("k1", bad));
      assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
    verify(service, never()).submit(anyString(), anyString(), anyString(), anyString());
  }

  @Test
  void missingFieldsAreRejected() {
    assertThat(
            controller
                .submit(new NotificationController.SubmitRequest(null, "email", "t", "b"))
                .getStatusCode())
        .isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(
            controller
                .submit(new NotificationController.SubmitRequest("k", "email", " ", "b"))
                .getStatusCode())
        .isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(controller.submit(null).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
  }

  @Test
  void publishFailureReturns503() {
    when(service.submit("k1", "sms", "user@example.com", "hi"))
        .thenReturn(new NotificationService.Submission(7L, "QUEUED", true));
    when(events.publish(7L)).thenReturn(false);

    assertThat(controller.submit(request("k1", "sms")).getStatusCode())
        .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
  }

  @Test
  void getReturnsStatusAndAttempts() {
    when(service.find(5L))
        .thenReturn(Optional.of(new NotificationService.Notification(5L, "sms", "SENT", 2)));

    ResponseEntity<NotificationController.NotificationResponse> res = controller.get(5L);

    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(res.getBody())
        .isEqualTo(new NotificationController.NotificationResponse(5L, "sms", "SENT", 2));
  }

  @Test
  void getUnknownIdIs404() {
    when(service.find(99L)).thenReturn(Optional.empty());
    assertThat(controller.get(99L).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
  }
}
