package com.systemdesign.realtimeauction.auctionbroadcast;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Fans every {@code realtime-auction.updates} record out to the auction's WebSocket watchers. The
 * consumer does no reshaping — the payload auction-api committed is the payload watchers receive —
 * so the Kafka partition order (key = auctionId) is preserved all the way to the browser (decision
 * 001). Malformed events are dropped rather than wedging the partition.
 */
@Component
class UpdateConsumer {

  private static final Logger log = LoggerFactory.getLogger(UpdateConsumer.class);

  private final AuctionSocketHandler handler;
  private final ObjectMapper mapper;

  UpdateConsumer(AuctionSocketHandler handler, ObjectMapper mapper) {
    this.handler = handler;
    this.mapper = mapper;
  }

  @KafkaListener(
      topics = "realtime-auction.updates",
      groupId = "${SD_KAFKA_GROUP_PREFIX:}broadcast")
  void onUpdate(String message) {
    JsonNode event;
    try {
      event = mapper.readTree(message);
    } catch (Exception e) {
      log.warn("dropping unparseable auction update: {}", message);
      return;
    }
    if (!event.path("auctionId").isNumber()) {
      log.warn("dropping auction update without auctionId: {}", message);
      return;
    }
    handler.broadcast(event.path("auctionId").asLong(), message);
  }
}
