package com.systemdesign.notification.notifyapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Exercises the idempotent submit path against real Postgres + Kafka: duplicate submits return the
 * same id and produce exactly one send event.
 */
@SpringBootTest
@Testcontainers
class NotifyApiIntegrationTest {

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(org.testcontainers.utility.DockerImageName.parse("apache/kafka:3.9.1"));

  @DynamicPropertySource
  static void props(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    registry.add("spring.kafka.listener.auto-startup", () -> "false");
  }

  @Autowired NotificationService service;
  @Autowired NotificationController controller;
  @Autowired JdbcClient jdbc;

  @Test
  void submitPersistsQueuedRowAndPublishesEvent() {
    ResponseEntity<?> res =
        controller.submit(
            new NotificationController.SubmitRequest("it-1", "email", "a@b.c", "hello"));

    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    NotificationController.SubmitResponse body =
        (NotificationController.SubmitResponse) res.getBody();
    assertThat(body.status()).isEqualTo("QUEUED");

    String status =
        jdbc.sql("SELECT status FROM notifications WHERE id = :id")
            .param("id", body.id())
            .query(String.class)
            .single();
    assertThat(status).isEqualTo("QUEUED");
  }

  @Test
  void duplicateSubmitReturnsSameIdAndPublishesOneEvent() throws Exception {
    var req = new NotificationController.SubmitRequest("it-dup", "sms", "+1555", "ping");

    ResponseEntity<?> first = controller.submit(req);
    ResponseEntity<?> second = controller.submit(req);

    assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
    long id = ((NotificationController.SubmitResponse) first.getBody()).id();
    assertThat(((NotificationController.SubmitResponse) second.getBody()).id()).isEqualTo(id);

    long rows =
        jdbc.sql("SELECT count(*) FROM notifications WHERE idempotency_key = 'it-dup'")
            .query(Long.class)
            .single();
    assertThat(rows).isEqualTo(1);

    // Exactly one send event reached the topic — the deduped submit must not requeue.
    try (KafkaConsumer<String, String> consumer =
        new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG,
                "notify-api-it",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"),
            new StringDeserializer(),
            new StringDeserializer())) {
      consumer.subscribe(java.util.List.of(NotificationEventPublisher.TOPIC));
      int events = 0;
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (System.nanoTime() < deadline) {
        for (var r : consumer.poll(java.time.Duration.ofMillis(500))) {
          if (r.value().contains("\"id\":" + id)) {
            events++;
          }
        }
      }
      assertThat(events).isEqualTo(1);
    }
  }
}
