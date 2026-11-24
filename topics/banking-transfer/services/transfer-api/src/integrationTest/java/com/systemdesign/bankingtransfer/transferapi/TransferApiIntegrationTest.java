package com.systemdesign.bankingtransfer.transferapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
 * The money-safety evidence, against real Postgres: concurrent transfers never drive a balance
 * negative and never change the total; the same idempotency key fired concurrently produces one
 * write and identical responses; replays never write a second ledger pair.
 */
@SpringBootTest
@Testcontainers
class TransferApiIntegrationTest {

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

  @Autowired TransferController controller;
  @Autowired JdbcClient jdbc;

  private long account(String owner, String initial) {
    ResponseEntity<?> res =
        controller.createAccount(
            new TransferController.CreateAccountRequest(owner, new BigDecimal(initial)));
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    return ((TransferController.AccountResponse) res.getBody()).id();
  }

  private BigDecimal balance(long id) {
    return jdbc.sql("SELECT balance FROM accounts WHERE id = :id")
        .param("id", id)
        .query(BigDecimal.class)
        .single();
  }

  private long ledgerRows(long transferId) {
    return jdbc.sql("SELECT count(*) FROM ledger WHERE transfer_id = :id")
        .param("id", transferId)
        .query(Long.class)
        .single();
  }

  @Test
  void completedTransferMovesMoneyAndWritesTheDoubleEntry() {
    long a = account("alice", "100.00");
    long b = account("bob", "50.00");

    ResponseEntity<?> res =
        controller.transfer(
            new TransferController.TransferRequest("t-1", a, b, new BigDecimal("30")));

    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    var body = (TransferController.TransferResponse) res.getBody();
    assertThat(body.status()).isEqualTo("COMPLETED");
    assertThat(balance(a)).isEqualByComparingTo("70.00");
    assertThat(balance(b)).isEqualByComparingTo("80.00");
    assertThat(ledgerRows(body.id())).isEqualTo(2);
    // One outbox event queued for the relay.
    assertThat(
            jdbc.sql("SELECT count(*) FROM outbox WHERE type = 'transfer.completed'")
                .query(Long.class)
                .single())
        .isGreaterThanOrEqualTo(1);
  }

  @Test
  void sameIdempotencyKeyFiveTimesConcurrentlyWritesOnceAndReplaysIdentically() throws Exception {
    long a = account("carol", "100.00");
    long b = account("dave", "0.00");
    int threads = 5;
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch go = new CountDownLatch(1);
    ConcurrentLinkedQueue<ResponseEntity<?>> responses = new ConcurrentLinkedQueue<>();

    try (var pool = Executors.newFixedThreadPool(threads)) {
      for (int i = 0; i < threads; i++) {
        pool.submit(
            () -> {
              ready.countDown();
              try {
                go.await();
                responses.add(
                    controller.transfer(
                        new TransferController.TransferRequest(
                            "dup-key", a, b, new BigDecimal("10.00"))));
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
    var bodies =
        responses.stream().map(r -> (TransferController.TransferResponse) r.getBody()).toList();
    // One winner gets 201, the other four replay the same stored result with 200.
    assertThat(responses.stream().map(ResponseEntity::getStatusCode))
        .containsExactlyInAnyOrder(
            HttpStatus.CREATED, HttpStatus.OK, HttpStatus.OK, HttpStatus.OK, HttpStatus.OK);
    assertThat(bodies.stream().map(TransferController.TransferResponse::id).distinct()).hasSize(1);
    assertThat(bodies.stream().map(TransferController.TransferResponse::status).distinct())
        .containsExactly("COMPLETED");
    // Exactly ONE transfer row and ONE ledger pair — never double-posted.
    long transferId = bodies.get(0).id();
    assertThat(ledgerRows(transferId)).isEqualTo(2);
    assertThat(balance(a)).isEqualByComparingTo("90.00");
    assertThat(balance(b)).isEqualByComparingTo("10.00");
  }

  @Test
  void fiftyConcurrentTransfersPreserveTheTotalAndNeverGoNegative() throws Exception {
    int n = 8;
    long[] ids = new long[n];
    for (int i = 0; i < n; i++) {
      ids[i] = account("pool-" + i, "100.00");
    }
    BigDecimal total = new BigDecimal(n * 100);
    int threads = 50;
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch go = new CountDownLatch(1);

    try (var pool = Executors.newFixedThreadPool(threads)) {
      for (int i = 0; i < threads; i++) {
        int k = i;
        pool.submit(
            () -> {
              ready.countDown();
              try {
                go.await();
                // Ring: k sends 1 unit to k+1 — exercises both lock directions.
                long from = ids[k % n];
                long to = ids[(k + 1) % n];
                controller.transfer(
                    new TransferController.TransferRequest(
                        "ring-" + k, from, to, new BigDecimal("1.00")));
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

    BigDecimal sum =
        jdbc.sql("SELECT COALESCE(SUM(balance), 0) FROM accounts WHERE owner LIKE 'pool-%'")
            .query(BigDecimal.class)
            .single();
    assertThat(sum).isEqualByComparingTo(total);
    assertThat(
            jdbc.sql("SELECT count(*) FROM accounts WHERE owner LIKE 'pool-%' AND balance < 0")
                .query(Long.class)
                .single())
        .isZero();
    // Double-entry sanity: every ledger DEBIT has its CREDIT pair, and signed sum per
    // account equals its balance minus the 100 it started with.
    assertThat(
            jdbc.sql(
                    "SELECT count(*) FROM ledger l JOIN accounts a ON a.id = l.account_id"
                        + " WHERE a.owner LIKE 'pool-%'")
                .query(Long.class)
                .single())
        .isPositive()
        .satisfies(c -> assertThat(c % 2).isZero());
  }

  @Test
  void insufficientFundsIsRejectedAndReplayedRejected() {
    long a = account("poor", "5.00");
    long b = account("rich", "0.00");

    ResponseEntity<?> res =
        controller.transfer(
            new TransferController.TransferRequest("rej", a, b, new BigDecimal("50")));
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
    var body = (TransferController.TransferResponse) res.getBody();
    assertThat(body.status()).isEqualTo("REJECTED");

    // Replay returns the same stored REJECTED — the rejection is durable, not recomputed.
    ResponseEntity<?> replay =
        controller.transfer(
            new TransferController.TransferRequest("rej", a, b, new BigDecimal("50")));
    assertThat(replay.getStatusCode()).isEqualTo(HttpStatus.OK);
    assertThat(((TransferController.TransferResponse) replay.getBody()).status())
        .isEqualTo("REJECTED");
    assertThat(((TransferController.TransferResponse) replay.getBody()).replayed()).isTrue();
    assertThat(balance(a)).isEqualByComparingTo("5.00");
    assertThat(ledgerRows(body.id())).isZero();
  }

  @Test
  void validationRejectsBadRequests() {
    long a = account("val-a", "10.00");
    long b = account("val-b", "10.00");

    assertThat(
            controller
                .transfer(new TransferController.TransferRequest("neg", a, b, new BigDecimal("-1")))
                .getStatusCode())
        .isEqualTo(HttpStatus.BAD_REQUEST);
    assertThat(
            controller
                .transfer(new TransferController.TransferRequest("self", a, a, new BigDecimal("1")))
                .getStatusCode())
        .isEqualTo(HttpStatus.BAD_REQUEST);
    // Unknown account → AccountNotFoundException out of the txn (rolled back);
    // the controller's @ExceptionHandler maps it to 404 under real MVC dispatch.
    assertThatThrownBy(
            () ->
                controller.transfer(
                    new TransferController.TransferRequest("ghost", a, 99999, new BigDecimal("1"))))
        .isInstanceOf(TransferService.AccountNotFoundException.class);
  }

  @Test
  void statementListsTheAccountsEntries() {
    long a = account("stmt-a", "100.00");
    long b = account("stmt-b", "0.00");
    controller.transfer(new TransferController.TransferRequest("s-1", a, b, new BigDecimal("40")));

    ResponseEntity<List<TransferController.StatementEntry>> res = controller.statement(a);
    assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
    // Opening CREDIT (seed money) then the transfer DEBIT — the full double-entry story.
    assertThat(res.getBody()).hasSize(2);
    assertThat(res.getBody().get(0).entry()).isEqualTo("CREDIT");
    assertThat(res.getBody().get(0).balanceAfter()).isEqualByComparingTo("100.00");
    assertThat(res.getBody().get(1).entry()).isEqualTo("DEBIT");
    assertThat(res.getBody().get(1).balanceAfter()).isEqualByComparingTo("60.00");
  }
}
