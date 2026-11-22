package com.systemdesign.urlshortener.redirect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class RedirectControllerTest {

  private RedirectService redirects;
  private ClickEventPublisher clicks;
  private RedirectController controller;

  @BeforeEach
  void setUp() {
    redirects = mock(RedirectService.class);
    clicks = mock(ClickEventPublisher.class);
    controller = new RedirectController(redirects, clicks);
  }

  @Test
  void foundReturns302AndPublishesClick() {
    when(redirects.resolve("abc")).thenReturn(Optional.of("https://example.com"));

    ResponseEntity<Void> response = controller.redirect("abc");

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FOUND);
    assertThat(response.getHeaders().getLocation().toString()).isEqualTo("https://example.com");
    verify(clicks).publish("abc");
  }

  @Test
  void missingReturns404AndDoesNotPublish() {
    when(redirects.resolve("nope")).thenReturn(Optional.empty());

    assertThat(controller.redirect("nope").getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    verify(clicks, never()).publish(anyString());
  }

  @Test
  void invalidCodeReturns400() {
    for (String bad : new String[] {"a-b", "with space", "waytoolongacode1234", "", "ünïcode"}) {
      assertThat(controller.redirect(bad).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
    verify(redirects, never()).resolve(anyString());
  }

  @Test
  void publishFailureStillReturns302() {
    // Real publisher with a KafkaTemplate that throws synchronously: failure is swallowed.
    org.springframework.kafka.core.KafkaTemplate<String, String> kafka = failingKafka();
    ClickEventPublisher realPublisher = new ClickEventPublisher(kafka);
    RedirectController resilient = new RedirectController(redirects, realPublisher);
    when(redirects.resolve("abc")).thenReturn(Optional.of("https://example.com"));

    ResponseEntity<Void> response = resilient.redirect("abc");

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FOUND);
    assertThat(response.getHeaders().getLocation().toString()).isEqualTo("https://example.com");
  }

  @SuppressWarnings("unchecked")
  private static org.springframework.kafka.core.KafkaTemplate<String, String> failingKafka() {
    org.springframework.kafka.core.KafkaTemplate<String, String> kafka =
        mock(org.springframework.kafka.core.KafkaTemplate.class);
    doThrow(new RuntimeException("kafka down"))
        .when(kafka)
        .send(anyString(), anyString(), anyString());
    return kafka;
  }
}
