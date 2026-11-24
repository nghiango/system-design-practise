package com.systemdesign.flashsale.seckillapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Runs Flyway migrations and exercises the seckill gate against real Postgres + Redis. The key
 * evidence: 200 racing buyers on stock=100 → exactly 100 accepted, never more.
 */
@SpringBootTest(
    properties = {"SD_REDIS_KEY_PREFIX=flash-sale:", "SD_KAFKA_GROUP_PREFIX=flash-sale."})
@Testcontainers
class SeckillApiIntegrationTest {

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

  @Container
  @SuppressWarnings("resource")
  static final GenericContainer<?> REDIS =
      new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(org.testcontainers.utility.DockerImageName.parse("apache/kafka:3.9.1"));

  @DynamicPropertySource
  static void props(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.data.redis.host", REDIS::getHost);
    registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.kafka.listener.auto-startup", () -> "false");
  }

  @Autowired ItemService items;
  @Autowired SaleService sales;
  @Autowired SeckillController controller;

  @Test
  void createItemPreloadsRedisStockAndLists() {
    ItemService.Item item = items.create("flash-widget", new BigDecimal("9.99"), 7);

    assertThat(items.stockKey(item.id())).startsWith("flash-sale:stock:");
    assertThat(items.list())
        .anySatisfy(
            view -> {
              assertThat(view.id()).isEqualTo(item.id());
              assertThat(view.stock()).isEqualTo(7);
            });
  }

  @Test
  void concurrentBuyersNeverOversell() throws InterruptedException {
    ItemService.Item item = items.create("race-item", new BigDecimal("1.00"), 100);

    int buyers = 200;
    // One thread per buyer: all 200 must reach the start line before the gate opens.
    ExecutorService pool = Executors.newFixedThreadPool(buyers);
    CountDownLatch ready = new CountDownLatch(buyers);
    CountDownLatch go = new CountDownLatch(1);
    ConcurrentLinkedQueue<SaleService.Result> results = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < buyers; i++) {
      int n = i;
      pool.submit(
          () -> {
            ready.countDown();
            try {
              go.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            results.add(sales.buy(item.id(), "order-" + n));
          });
    }
    ready.await();
    go.countDown();
    pool.shutdown();
    assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();

    List<SaleService.Result> all = new ArrayList<>(results);
    assertThat(all).hasSize(buyers);
    // The headline evidence: exactly 100 accepted, 100 sold out — never more than the seeded stock.
    assertThat(all.stream().filter(r -> r == SaleService.Result.ACCEPTED).count()).isEqualTo(100);
    assertThat(all.stream().filter(r -> r == SaleService.Result.SOLD_OUT).count()).isEqualTo(100);
  }

  @Test
  void unknownItemIsNotFound() {
    assertThat(sales.buy(999_999L, "order-x")).isEqualTo(SaleService.Result.ITEM_NOT_FOUND);
  }
}
