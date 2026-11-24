package com.systemdesign.notification.senderworker;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * End-to-end evidence against real Postgres + Redis + Kafka: the live {@code @KafkaListener} drains
 * {@code notification.requested} through the Redis token bucket and the simulated provider — normal
 * targets reach SENT, a flaky target reaches SENT via the retry topic, and a permanently failing
 * target lands on DEAD.
 */
@SpringBootTest(properties = {"SD_REDIS_KEY_PREFIX=notification:", "SD_KAFKA_GROUP_PREFIX=it."})
@Testcontainers
class SenderWorkerIntegrationTest {

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
    registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest");
  }

  @Autowired JdbcClient jdbc;

  private long insert(String key, String channel, String target) {
    return jdbc.sql(
            "INSERT INTO notifications (idempotency_key, channel, target, body) VALUES"
                + " (:key, :channel, :target, 'b') RETURNING id")
        .param("key", key)
        .param("channel", channel)
        .param("target", target)
        .query(Long.class)
        .single();
  }

  private String statusOf(long id) {
    return jdbc.sql("SELECT status FROM notifications WHERE id = :id")
        .param("id", id)
        .query(String.class)
        .single();
  }

  private int attemptsOf(long id) {
    return jdbc.sql("SELECT attempts FROM notifications WHERE id = :id")
        .param("id", id)
        .query(Integer.class)
        .single();
  }

  @Test
  void notificationsReachTerminalState() throws Exception {
    List<Long> normal = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      normal.add(insert("it-n-" + i, "email", "user" + i + "@example.com"));
    }
    long flaky = insert("it-flaky", "email", "flaky-device@example.com");
    long fail = insert("it-fail", "email", "fail@nowhere.example");

    try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(
            Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()),
            new StringSerializer(),
            new StringSerializer())) {
      for (long id : normal) {
        producer
            .send(
                new ProducerRecord<>(
                    SendConsumer.REQUESTED_TOPIC,
                    Long.toString(id),
                    "{\"id\":" + id + ",\"attempt\":0}"))
            .get();
      }
      for (long id : new long[] {flaky, fail}) {
        producer
            .send(
                new ProducerRecord<>(
                    SendConsumer.REQUESTED_TOPIC,
                    Long.toString(id),
                    "{\"id\":" + id + ",\"attempt\":0}"))
            .get();
      }
    }

    // 22 sends through a 10 tokens/s bucket — give the consumer slack to drain.
    long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
    while (System.nanoTime() < deadline) {
      boolean terminal = true;
      for (long id : normal) {
        terminal &= "SENT".equals(statusOf(id));
      }
      terminal &= "SENT".equals(statusOf(flaky));
      terminal &= "DEAD".equals(statusOf(fail));
      if (terminal) {
        break;
      }
      Thread.sleep(500);
    }

    for (long id : normal) {
      assertThat(statusOf(id)).as("notification %s", id).isEqualTo("SENT");
    }
    // Retry evidence: flaky went through the retry topic at least once before SENT.
    assertThat(statusOf(flaky)).isEqualTo("SENT");
    assertThat(attemptsOf(flaky)).isGreaterThanOrEqualTo(1);
    assertThat(statusOf(fail)).isEqualTo("DEAD");
  }
}
