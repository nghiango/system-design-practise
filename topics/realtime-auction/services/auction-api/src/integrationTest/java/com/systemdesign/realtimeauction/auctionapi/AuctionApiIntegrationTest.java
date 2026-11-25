package com.systemdesign.realtimeauction.auctionapi;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
 * The ordering evidence, against real Postgres and Kafka: 100 bids fired at one auction
 * concurrently are serialised by the row lock, the highest bid equals the maximum amount offered,
 * the accepted seqs are contiguous 1..N, and the Kafka records for the auction arrive in that same
 * order — the property the broadcaster relies on.
 */
@SpringBootTest
@Testcontainers
class AuctionApiIntegrationTest {

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

  @Autowired AuctionController controller;
  @Autowired JdbcClient jdbc;

  private long auction(String title, String startPrice, long endsInSec) {
    ResponseEntity<?> res =
        controller.createAuction(
            new AuctionController.CreateAuctionRequest(
                title, new BigDecimal(startPrice), endsInSec));
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    return ((AuctionController.AuctionResponse) res.getBody()).id();
  }

  @Test
  void hundredConcurrentBidsSerialiseToContiguousSeqsAndMaxAmountWins() throws Exception {
    long auctionId = auction("race", "0.00", 300);
    int threads = 100;
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch go = new CountDownLatch(1);
    ConcurrentLinkedQueue<ResponseEntity<?>> responses = new ConcurrentLinkedQueue<>();

    try (var pool = Executors.newFixedThreadPool(threads)) {
      for (int i = 1; i <= threads; i++) {
        int amount = i; // every bidder offers a distinct amount 1..100
        pool.submit(
            () -> {
              ready.countDown();
              try {
                go.await();
                responses.add(
                    controller.bid(
                        auctionId,
                        new AuctionController.BidRequest(
                            "bidder-" + amount, new BigDecimal(amount))));
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

    assertThat(responses).hasSize(threads);
    for (ResponseEntity<?> res : responses) {
      assertThat(res.getStatusCode()).isIn(HttpStatus.OK, HttpStatus.CONFLICT);
    }

    // The max amount offered must have won — the lock made every comparison exact.
    var state = (AuctionController.AuctionState) controller.auction(auctionId).getBody();
    assertThat(state.highestBid()).isEqualByComparingTo("100");
    assertThat(state.highestBidder()).isEqualTo("bidder-100");
    assertThat(state.status()).isEqualTo("OPEN");

    // seq is a contiguous 1..N over the accepted bids and equals bid_count.
    ResponseEntity<List<AuctionController.BidEntry>> bidsRes = controller.bids(auctionId);
    List<AuctionController.BidEntry> bids = bidsRes.getBody();
    assertThat(bids).hasSize(state.bidCount());
    assertThat(bids.stream().map(AuctionController.BidEntry::seq))
        .containsExactlyElementsOf(seqRange(state.bidCount()));
    // Accepted amounts strictly increase with seq — every accepted bid beat the previous highest.
    assertThat(bids).isSortedAccordingTo(Comparator.comparing(AuctionController.BidEntry::amount));

    // Same order on the wire: every Kafka record for this auction is keyed by auctionId
    // (→ one partition) and its seq values arrive in the lock-assigned order.
    List<String> records = consumeUpdates(auctionId);
    assertThat(records).hasSize(state.bidCount());
    List<Integer> kafkaSeqs =
        records.stream()
            .map(r -> Integer.valueOf(r.replaceAll(".*\"seq\":(\\d+).*", "$1")))
            .toList();
    assertThat(kafkaSeqs).isSorted();
    assertThat(kafkaSeqs).containsExactlyElementsOf(seqRange(state.bidCount()));
  }

  private List<Integer> seqRange(int n) {
    List<Integer> range = new ArrayList<>();
    for (int i = 1; i <= n; i++) {
      range.add(i);
    }
    return range;
  }

  private List<String> consumeUpdates(long auctionId) {
    Properties props = new Properties();
    props.put("bootstrap.servers", KAFKA.getBootstrapServers());
    props.put("group.id", "auction-it-" + System.nanoTime());
    props.put("auto.offset.reset", "earliest");
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());
    List<String> values = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of("realtime-auction.updates"));
      long deadline = System.currentTimeMillis() + 30_000;
      while (System.currentTimeMillis() < deadline) {
        var polled = consumer.poll(Duration.ofMillis(500));
        for (var record : polled) {
          if (String.valueOf(auctionId).equals(record.key())) {
            values.add(record.value());
          }
        }
        if (values.size() >= 1 && polled.isEmpty()) {
          break;
        }
      }
    }
    return values;
  }

  @Test
  void firstBidAfterTheDeadlineClosesTheAuctionForEveryoneAfterIt() {
    long auctionId = auction("short", "10.00", 1);
    try {
      Thread.sleep(1500);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    ResponseEntity<?> res =
        controller.bid(auctionId, new AuctionController.BidRequest("late", new BigDecimal("50")));
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.GONE);

    var state = (AuctionController.AuctionState) controller.auction(auctionId).getBody();
    assertThat(state.status()).isEqualTo("CLOSED");

    ResponseEntity<?> second =
        controller.bid(auctionId, new AuctionController.BidRequest("later", new BigDecimal("60")));
    assertThat(second.getStatusCode()).isEqualTo(HttpStatus.GONE);
    assertThat(
            jdbc.sql("SELECT count(*) FROM bids WHERE auction_id = :id")
                .param("id", auctionId)
                .query(Long.class)
                .single())
        .isZero();
  }

  @Test
  void equalConcurrentBidsFirstWinsSecondIsRejected() throws Exception {
    long auctionId = auction("tie", "0.00", 300);
    int threads = 8;
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch go = new CountDownLatch(1);
    ConcurrentLinkedQueue<ResponseEntity<?>> responses = new ConcurrentLinkedQueue<>();

    try (var pool = Executors.newFixedThreadPool(threads)) {
      for (int i = 0; i < threads; i++) {
        int k = i;
        pool.submit(
            () -> {
              ready.countDown();
              try {
                go.await();
                // Same amount every time — the race is decided purely by lock order.
                responses.add(
                    controller.bid(
                        auctionId,
                        new AuctionController.BidRequest("t" + k, new BigDecimal("42.00"))));
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

    assertThat(responses.stream().map(ResponseEntity::getStatusCode).toList())
        .containsExactlyInAnyOrder(
            HttpStatus.OK,
            HttpStatus.CONFLICT,
            HttpStatus.CONFLICT,
            HttpStatus.CONFLICT,
            HttpStatus.CONFLICT,
            HttpStatus.CONFLICT,
            HttpStatus.CONFLICT,
            HttpStatus.CONFLICT);
    assertThat(
            jdbc.sql("SELECT count(*) FROM bids WHERE auction_id = :id AND accepted = false")
                .param("id", auctionId)
                .query(Long.class)
                .single())
        .isEqualTo(threads - 1);
  }

  @Test
  void missingAndValidationEdges() {
    assertThat(
            controller
                .bid(99999L, new AuctionController.BidRequest("ghost", new BigDecimal("1")))
                .getStatusCode())
        .isEqualTo(HttpStatus.NOT_FOUND);
    assertThat(controller.auction(99999L).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    assertThat(controller.bids(99999L).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

    long auctionId = auction("val", "10.00", 300);
    assertThat(
            controller
                .bid(auctionId, new AuctionController.BidRequest("bad", new BigDecimal("-1")))
                .getStatusCode())
        .isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(
            controller
                .createAuction(
                    new AuctionController.CreateAuctionRequest("", new BigDecimal("1"), 60))
                .getStatusCode())
        .isEqualTo(HttpStatus.BAD_REQUEST);
  }
}
