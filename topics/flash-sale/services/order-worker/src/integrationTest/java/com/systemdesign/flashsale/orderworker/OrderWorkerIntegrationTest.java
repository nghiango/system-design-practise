package com.systemdesign.flashsale.orderworker;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Exercises idempotent order persistence and drift compensation against real Postgres + Redis. */
@SpringBootTest(
    properties = {"SD_REDIS_KEY_PREFIX=flash-sale:", "SD_KAFKA_GROUP_PREFIX=flash-sale."})
@Testcontainers
class OrderWorkerIntegrationTest {

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

  @Container
  @SuppressWarnings("resource")
  static final GenericContainer<?> REDIS =
      new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

  @DynamicPropertySource
  static void props(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.data.redis.host", REDIS::getHost);
    registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    registry.add("spring.kafka.bootstrap-servers", () -> "localhost:1");
    registry.add("spring.kafka.listener.auto-startup", () -> "false");
  }

  @Autowired JdbcClient jdbc;
  @Autowired StringRedisTemplate redis;
  @Autowired OrderConsumer consumer;

  private long createItem(int stock) {
    return jdbc.sql(
            "INSERT INTO items (name, price, stock) VALUES ('t', 1.00, :stock) RETURNING id")
        .param("stock", stock)
        .query(Long.class)
        .single();
  }

  private int dbStock(long itemId) {
    return jdbc.sql("SELECT stock FROM items WHERE id = :id")
        .param("id", itemId)
        .query(Integer.class)
        .single();
  }

  private String orderStatus(String orderId) {
    return jdbc.sql("SELECT status FROM orders WHERE order_id = :id")
        .param("id", orderId)
        .query(String.class)
        .single();
  }

  @Test
  void duplicateDeliveryPersistsOnceAndDecrementsOnce() {
    long itemId = createItem(10);
    String message = "{\"orderId\":\"dup-1\",\"itemId\":" + itemId + "}";

    consumer.onOrder(message);
    consumer.onOrder(message);

    assertThat(orderStatus("dup-1")).isEqualTo("created");
    assertThat(dbStock(itemId)).isEqualTo(9);
  }

  @Test
  void dbStockDriftMarksOrderFailedAndReturnsUnitToRedis() {
    long itemId = createItem(0); // Postgres says sold out but Redis gate let an order through.
    redis.opsForValue().set("flash-sale:stock:" + itemId, "0");

    consumer.onOrder("{\"orderId\":\"drift-1\",\"itemId\":" + itemId + "}");

    assertThat(orderStatus("drift-1")).isEqualTo("failed");
    assertThat(redis.opsForValue().get("flash-sale:stock:" + itemId)).isEqualTo("1");
  }
}
