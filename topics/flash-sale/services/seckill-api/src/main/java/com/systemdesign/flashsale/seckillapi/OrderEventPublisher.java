package com.systemdesign.flashsale.seckillapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes {@code {"orderId","itemId"}} after the Redis gate accepted a sale. Unlike the
 * url-shortener click events this publish is <em>not</em> best-effort: the caller compensates (INCR
 * the stock back) when it fails, because a lost order event means paid inventory never reaches
 * Postgres.
 */
@Component
class OrderEventPublisher {

  static final String TOPIC = "flash-sale.order.created";

  private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);

  private final KafkaTemplate<String, String> kafka;

  OrderEventPublisher(KafkaTemplate<String, String> kafka) {
    this.kafka = kafka;
  }

  /** Returns {@code true} when the event was handed to the broker. */
  boolean publish(String orderId, long itemId) {
    try {
      kafka
          .send(TOPIC, orderId, "{\"orderId\":\"" + orderId + "\",\"itemId\":" + itemId + "}")
          .get();
      return true;
    } catch (Exception e) {
      log.warn("order event publish failed for {}: {}", orderId, e.toString());
      return false;
    }
  }
}
