package com.systemdesign.ticketbooking.bookingapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
 * The race-condition evidence: real Postgres + Kafka, 100 threads holding the same seat → exactly
 * one wins; expired holds are reclaimable; confirm after expiry → 410; decline → seat FREE +
 * DECLINED payment.
 */
@SpringBootTest
@Testcontainers
class BookingApiIntegrationTest {

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

  @Autowired BookingController controller;
  @Autowired JdbcClient jdbc;

  private long createEvent(int rows, int cols) {
    ResponseEntity<?> res =
        controller.createEvent(
            new BookingController.CreateEventRequest("gig", new BigDecimal("49.90"), rows, cols));
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    return ((BookingController.EventResponse) res.getBody()).id();
  }

  private void expireHold(long eventId, String seat) {
    jdbc.sql(
            "UPDATE seats SET hold_expires_at = now() - interval '1 second'"
                + " WHERE event_id = :e AND seat = :s")
        .param("e", eventId)
        .param("s", seat)
        .update();
  }

  private String seatStatus(long eventId, String seat) {
    return jdbc.sql("SELECT status FROM seats WHERE event_id = :e AND seat = :s")
        .param("e", eventId)
        .param("s", seat)
        .query(String.class)
        .single();
  }

  @Test
  void createEventGeneratesTheFullSeatMap() {
    long id = createEvent(2, 3);

    ResponseEntity<?> res = controller.seats(id);
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
    @SuppressWarnings("unchecked")
    var seats = (java.util.List<BookingController.SeatResponse>) res.getBody();
    assertThat(seats).hasSize(6);
    assertThat(seats.stream().map(BookingController.SeatResponse::seat))
        .containsExactly("A1", "A2", "A3", "B1", "B2", "B3");
    assertThat(seats.stream().map(BookingController.SeatResponse::status)).containsOnly("FREE");
  }

  @Test
  void hundredThreadsHoldTheSameSeatExactlyOneWins() throws Exception {
    long id = createEvent(10, 10);
    int threads = 100;
    AtomicInteger wins = new AtomicInteger();
    AtomicInteger conflicts = new AtomicInteger();
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch go = new CountDownLatch(1);

    try (var pool = Executors.newFixedThreadPool(threads)) {
      for (int i = 0; i < threads; i++) {
        int n = i;
        pool.submit(
            () -> {
              ready.countDown();
              try {
                go.await();
                ResponseEntity<?> res =
                    controller.hold(id, new BookingController.HoldRequest("E5", "racer-" + n));
                if (res.getStatusCode() == HttpStatus.OK) {
                  wins.incrementAndGet();
                } else if (res.getStatusCode() == HttpStatus.CONFLICT) {
                  conflicts.incrementAndGet();
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
      }
      assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue();
      go.countDown();
      pool.shutdown();
      assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(wins.get()).isEqualTo(1);
    assertThat(conflicts.get()).isEqualTo(threads - 1);
    assertThat(seatStatus(id, "E5")).isEqualTo("HELD");
  }

  @Test
  void expiredHoldCanBeTakenByAnotherOwner() {
    long id = createEvent(1, 2);

    assertThat(
            controller.hold(id, new BookingController.HoldRequest("A1", "first")).getStatusCode())
        .isEqualTo(HttpStatus.OK);
    expireHold(id, "A1");

    // The seat map already reads the expired hold as FREE (filter-on-read).
    @SuppressWarnings("unchecked")
    var seats = (java.util.List<BookingController.SeatResponse>) controller.seats(id).getBody();
    assertThat(seats.stream().filter(s -> s.seat().equals("A1")).findFirst().orElseThrow().status())
        .isEqualTo("FREE");

    ResponseEntity<?> res = controller.hold(id, new BookingController.HoldRequest("A1", "second"));
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(
            jdbc.sql("SELECT hold_owner FROM seats WHERE event_id = :e AND seat = 'A1'")
                .param("e", id)
                .query(String.class)
                .single())
        .isEqualTo("second");
  }

  @Test
  void confirmBooksTheSeatAndRecordsPayment() {
    long id = createEvent(1, 1);
    controller.hold(id, new BookingController.HoldRequest("A1", "buyer"));

    ResponseEntity<?> res =
        controller.confirm(id, new BookingController.ConfirmRequest("A1", "buyer"));

    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(seatStatus(id, "A1")).isEqualTo("BOOKED");
    assertThat(
            jdbc.sql("SELECT status FROM payments WHERE event_id = :e AND seat = 'A1'")
                .param("e", id)
                .query(String.class)
                .single())
        .isEqualTo("PAID");
  }

  @Test
  void confirmAfterExpiryIsGone() {
    long id = createEvent(1, 1);
    controller.hold(id, new BookingController.HoldRequest("A1", "slow"));
    expireHold(id, "A1");

    ResponseEntity<?> res =
        controller.confirm(id, new BookingController.ConfirmRequest("A1", "slow"));

    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.GONE);
    assertThat(seatStatus(id, "A1")).isEqualTo("HELD"); // row still held until swept
  }

  @Test
  void declinedPaymentReleasesTheSeat() {
    long id = createEvent(1, 1);
    controller.hold(id, new BookingController.HoldRequest("A1", "decline-me"));

    ResponseEntity<?> res =
        controller.confirm(id, new BookingController.ConfirmRequest("A1", "decline-me"));

    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.PAYMENT_REQUIRED);
    assertThat(seatStatus(id, "A1")).isEqualTo("FREE");
    assertThat(
            jdbc.sql("SELECT status FROM payments WHERE event_id = :e AND seat = 'A1'")
                .param("e", id)
                .query(String.class)
                .single())
        .isEqualTo("DECLINED");
  }

  @Test
  void releaseIsOwnerOnly() {
    long id = createEvent(1, 1);
    controller.hold(id, new BookingController.HoldRequest("A1", "alice"));

    assertThat(
            controller
                .release(id, new BookingController.HoldRequest("A1", "mallory"))
                .getStatusCode())
        .isEqualTo(HttpStatus.CONFLICT);
    assertThat(
            controller
                .release(id, new BookingController.HoldRequest("A1", "alice"))
                .getStatusCode())
        .isEqualTo(HttpStatus.NO_CONTENT);
    assertThat(seatStatus(id, "A1")).isEqualTo("FREE");
  }
}
