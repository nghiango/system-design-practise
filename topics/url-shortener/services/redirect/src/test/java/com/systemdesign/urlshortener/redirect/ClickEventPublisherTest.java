package com.systemdesign.urlshortener.redirect;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

class ClickEventPublisherTest {

  @Test
  @SuppressWarnings("unchecked")
  void swallowsSynchronousSendFailure() {
    KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    when(kafka.send(anyString(), anyString(), anyString()))
        .thenThrow(new RuntimeException("broker down"));

    ClickEventPublisher publisher = new ClickEventPublisher(kafka);
    assertThatCode(() -> publisher.publish("abc")).doesNotThrowAnyException();
  }

  @Test
  @SuppressWarnings("unchecked")
  void swallowsAsyncSendFailure() {
    KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RuntimeException("timeout"));
    when(kafka.send(anyString(), anyString(), anyString())).thenReturn(failed);

    ClickEventPublisher publisher = new ClickEventPublisher(kafka);
    assertThatCode(() -> publisher.publish("abc")).doesNotThrowAnyException();
  }
}
