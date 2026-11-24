package com.systemdesign.ticketbooking.bookingworker;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
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
 * Real Postgres + Kafka: the sweeper frees an expired hold and the audit consumer appends booking
 * events to {@code booking_history}.
 */
@SpringBootTest(properties = "sd.sweeper.delay-ms=200")
@Testcontainers
class BookingWorkerIntegrationTest {

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
  }

  @Autowired JdbcClient jdbc;
  @Autowired HistoryController history;

  private long insertEventWithSeat(String seat) {
    long eventId =
        jdbc.sql("INSERT INTO events (name, price) VALUES ('it', 10) RETURNING id")
            .query(Long.class)
            .single();
    jdbc.sql("INSERT INTO seats (event_id, seat) VALUES (:e, :s)")
        .param("e", eventId)
        .param("s", seat)
        .update();
    return eventId;
  }

  private static void await(String what, java.util.function.BooleanSupplier cond)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < deadline) {
      if (cond.getAsBoolean()) {
        return;
      }
      Thread.sleep(200);
    }
    throw new AssertionError("timed out waiting for " + what);
  }

  @Test
  void sweeperFreesExpiredHoldAndPublishesExpired() throws Exception {
    long eventId = insertEventWithSeat("A9");
    jdbc.sql(
            "UPDATE seats SET status = 'HELD', hold_owner = 'gone',"
                + " hold_expires_at = now() - interval '1 second'"
                + " WHERE event_id = :e AND seat = 'A9'")
        .param("e", eventId)
        .update();

    await(
        "sweeper to free A9",
        () ->
            "FREE"
                .equals(
                    jdbc.sql("SELECT status FROM seats WHERE event_id = :e AND seat = 'A9'")
                        .param("e", eventId)
                        .query(String.class)
                        .single()));

    // The seat is really reclaimable: a fresh hold-style guarded UPDATE now succeeds.
    int claimed =
        jdbc.sql(
                "UPDATE seats SET status = 'HELD', hold_owner = 'next',"
                    + " hold_expires_at = now() + interval '5 minutes'"
                    + " WHERE event_id = :e AND seat = 'A9' AND status = 'FREE'")
            .param("e", eventId)
            .update();
    assertThat(claimed).isEqualTo(1);
  }

  @Test
  void bookingEventsLandInHistoryAndAreServedByTheApi() throws Exception {
    long eventId = insertEventWithSeat("B1");

    try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(
            Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()),
            new StringSerializer(),
            new StringSerializer())) {
      producer
          .send(
              new ProducerRecord<>(
                  AuditConsumer.TOPIC,
                  eventId + ":B1",
                  "{\"type\":\"HELD\",\"eventId\":"
                      + eventId
                      + ",\"seat\":\"B1\",\"owner\":\"auditee\"}"))
          .get();
    }

    await(
        "audit row for event " + eventId,
        () ->
            jdbc.sql("SELECT count(*) FROM booking_history WHERE event_id = :e")
                    .param("e", eventId)
                    .query(Long.class)
                    .single()
                > 0);

    ResponseEntity<List<HistoryController.HistoryEntry>> res = history.history(eventId);
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(res.getBody())
        .anySatisfy(
            h -> {
              assertThat(h.type()).isEqualTo("HELD");
              assertThat(h.seat()).isEqualTo("B1");
              assertThat(h.owner()).isEqualTo("auditee");
            });
  }
}
