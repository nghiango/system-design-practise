package com.systemdesign.realtimeauction.auctionapi;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

class BidEventPublisherTest {

  @Test
  @SuppressWarnings("unchecked")
  void publishesKeyedByAuctionIdSoPartitionOrderEqualsBidOrder() {
    KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    when(kafka.send(anyString(), anyString(), anyString()))
        .thenReturn(new CompletableFuture<SendResult<String, String>>());

    new BidEventPublisher(kafka)
        .publish(7L, new BigDecimal("41.00"), "bob", 3, OffsetDateTime.now());

    verify(kafka)
        .send(
            eq("realtime-auction.updates"),
            eq("7"),
            org.mockito.ArgumentMatchers.contains("\"seq\":3"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void swallowsSendFailureBecauseTheBidRowIsAlreadyCommitted() {
    KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RuntimeException("broker down"));
    when(kafka.send(anyString(), anyString(), anyString())).thenReturn(failed);

    new BidEventPublisher(kafka)
        .publish(7L, new BigDecimal("41.00"), "bob", 3, OffsetDateTime.now());
    // No exception escapes — a lost live tick never fails the bid.
  }
}
