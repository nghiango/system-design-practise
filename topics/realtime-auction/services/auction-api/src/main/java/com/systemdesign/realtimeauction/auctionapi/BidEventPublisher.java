package com.systemdesign.realtimeauction.auctionapi;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes accepted bids to {@code realtime-auction.updates} keyed by the auction id. The key is
 * the whole ordering guarantee: Kafka routes one key to one partition, and a partition is ordered —
 * so the broadcast consumers see an auction's bids in exactly the seq order the row lock assigned
 * (decision 001). Delivery to watchers is best-effort: a failed send only loses a live tick, never
 * the bid itself, which is already committed to {@code bids}.
 */
@Component
class BidEventPublisher {

  static final String TOPIC = "realtime-auction.updates";

  private static final Logger log = LoggerFactory.getLogger(BidEventPublisher.class);

  private final KafkaTemplate<String, String> kafka;

  BidEventPublisher(KafkaTemplate<String, String> kafka) {
    this.kafka = kafka;
  }

  void publish(long auctionId, BigDecimal amount, String bidder, int seq, OffsetDateTime endsAt) {
    String payload =
        "{\"auctionId\":"
            + auctionId
            + ",\"amount\":"
            + amount
            + ",\"bidder\":\""
            + bidder.replace("\"", "\\\"")
            + "\",\"seq\":"
            + seq
            + ",\"endsAt\":\""
            + endsAt
            + "\"}";
    try {
      kafka
          .send(TOPIC, String.valueOf(auctionId), payload)
          .whenComplete(
              (result, error) -> {
                if (error != null) {
                  log.warn(
                      "bid update publish failed for auction {}: {}", auctionId, error.toString());
                }
              });
    } catch (Exception e) {
      log.warn("bid update publish failed for auction {}: {}", auctionId, e.toString());
    }
  }
}
