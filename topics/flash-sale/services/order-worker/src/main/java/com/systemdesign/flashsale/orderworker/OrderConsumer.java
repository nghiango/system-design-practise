package com.systemdesign.flashsale.orderworker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Consumes {@code flash-sale.order.created} events and persists each order. Idempotent: the {@code
 * orders.order_id} primary key plus {@code ON CONFLICT DO NOTHING} makes at-least-once delivery
 * safe.
 *
 * <p>Compensation: when the guarded {@code UPDATE items SET stock=stock-1 WHERE stock>0} affects
 * zero rows the Postgres stock has drifted below the Redis counter. The worker then INCRs the Redis
 * counter back and marks the order {@code failed} rather than letting Redis keep selling phantom
 * stock (saga-style compensation, see docs/decisions/002).
 */
@Component
class OrderConsumer {

  static final String TOPIC = "flash-sale.order.created";

  private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

  private final JdbcClient jdbc;
  private final StringRedisTemplate redis;
  private final ObjectMapper mapper;
  private final String keyPrefix;

  OrderConsumer(
      JdbcClient jdbc,
      StringRedisTemplate redis,
      ObjectMapper mapper,
      @Value("${SD_REDIS_KEY_PREFIX:flash-sale:}") String keyPrefix) {
    this.jdbc = jdbc;
    this.redis = redis;
    this.mapper = mapper;
    this.keyPrefix = keyPrefix;
  }

  @KafkaListener(topics = TOPIC, groupId = "${SD_KAFKA_GROUP_PREFIX:}order-worker")
  void onOrder(String message) {
    OrderEvent event = extract(message);
    if (event == null) {
      return;
    }
    persist(event);
  }

  OrderEvent extract(String message) {
    try {
      JsonNode node = mapper.readTree(message);
      JsonNode orderId = node.get("orderId");
      JsonNode itemId = node.get("itemId");
      if (orderId == null
          || !orderId.isString()
          || orderId.asString().isBlank()
          || itemId == null
          || !itemId.isNumber()) {
        return null;
      }
      return new OrderEvent(orderId.asString(), itemId.asLong());
    } catch (Exception e) {
      return null;
    }
  }

  void persist(OrderEvent event) {
    int inserted =
        jdbc.sql(
                "INSERT INTO orders (order_id, item_id, status) VALUES (:orderId, :itemId,"
                    + " 'created') ON CONFLICT (order_id) DO NOTHING")
            .param("orderId", event.orderId())
            .param("itemId", event.itemId())
            .update();
    if (inserted == 0) {
      // Duplicate delivery — already persisted.
      return;
    }

    int decremented =
        jdbc.sql("UPDATE items SET stock = stock - 1 WHERE id = :id AND stock > 0")
            .param("id", event.itemId())
            .update();
    if (decremented == 0) {
      compensate(event);
    }
  }

  /** Returns the unit to Redis and marks the order failed when Postgres stock has drifted. */
  void compensate(OrderEvent event) {
    jdbc.sql("UPDATE orders SET status = 'failed' WHERE order_id = :orderId")
        .param("orderId", event.orderId())
        .update();
    try {
      redis.opsForValue().increment(keyPrefix + "stock:" + event.itemId());
    } catch (Exception e) {
      log.error(
          "compensation INCR failed for item {} — manual stock reconciliation needed: {}",
          event.itemId(),
          e.toString());
    }
    log.warn("order {} failed: postgres stock drift for item {}", event.orderId(), event.itemId());
  }

  record OrderEvent(String orderId, long itemId) {}
}
