package com.systemdesign.bankingtransfer.ledgerworker;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
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
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The full recovery path on real Postgres + Kafka: an unpublished outbox row is relayed to {@code
 * banking-transfer.ledger.entries}, the projection consumer writes {@code ledger_audit} (deduped),
 * and {@code /api/reconciliation} proves balance == ledger == projection — zero drift.
 */
@SpringBootTest(properties = "sd.relay.delay-ms=200")
@Testcontainers
class LedgerWorkerIntegrationTest {

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
  @Autowired ReconciliationController reconciliation;

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
  void outboxRowIsRelayedProjectedAndReconcilesToZeroDrift() throws Exception {
    long a =
        jdbc.sql("INSERT INTO accounts (owner, balance) VALUES ('w-a', 90.00) RETURNING id")
            .query(Long.class)
            .single();
    long b =
        jdbc.sql("INSERT INTO accounts (owner, balance) VALUES ('w-b', 110.00) RETURNING id")
            .query(Long.class)
            .single();
    // Opening entries (transfer_id NULL) — the seed money the ledger must also explain.
    jdbc.sql(
            "INSERT INTO ledger (transfer_id, account_id, entry, amount, balance_after)"
                + " VALUES (NULL, :a, 'CREDIT', 100.00, 100.00),"
                + " (NULL, :b, 'CREDIT', 100.00, 100.00)")
        .param("a", a)
        .param("b", b)
        .update();
    for (long accountId : new long[] {a, b}) {
      jdbc.sql("INSERT INTO outbox (type, payload) VALUES ('account.opened', :p)")
          .param(
              "p",
              "{\"type\":\"account.opened\",\"transferId\":0,\"entries\":[{"
                  + "\"accountId\":"
                  + accountId
                  + ",\"entry\":\"CREDIT\",\"amount\":100.00,\"balanceAfter\":100.00}]}")
          .update();
    }
    long transferId =
        jdbc.sql(
                "INSERT INTO transfers (idempotency_key, from_account, to_account, amount, status)"
                    + " VALUES ('w-t', :a, :b, 10.00, 'COMPLETED') RETURNING id")
            .param("a", a)
            .param("b", b)
            .query(Long.class)
            .single();
    jdbc.sql(
            "INSERT INTO ledger (transfer_id, account_id, entry, amount, balance_after)"
                + " VALUES (:t, :a, 'DEBIT', 10.00, 90.00), (:t, :b, 'CREDIT', 10.00, 110.00)")
        .param("t", transferId)
        .param("a", a)
        .param("b", b)
        .update();
    String payload =
        "{\"type\":\"transfer.completed\",\"transferId\":"
            + transferId
            + ",\"entries\":["
            + "{\"accountId\":"
            + a
            + ",\"entry\":\"DEBIT\",\"amount\":10.00,\"balanceAfter\":90.00},"
            + "{\"accountId\":"
            + b
            + ",\"entry\":\"CREDIT\",\"amount\":10.00,\"balanceAfter\":110.00}]}";
    jdbc.sql("INSERT INTO outbox (type, payload) VALUES ('transfer.completed', :p)")
        .param("p", payload)
        .update();

    await(
        "all outbox rows published",
        () ->
            jdbc.sql("SELECT count(*) FROM outbox WHERE NOT published").query(Long.class).single()
                == 0);
    await(
        "ledger_audit projection",
        () ->
            jdbc.sql("SELECT count(*) FROM ledger_audit WHERE account_id IN (:a, :b)")
                    .param("a", a)
                    .param("b", b)
                    .query(Long.class)
                    .single()
                == 4);

    // A duplicate delivery (outbox republish after crash, Kafka redelivery) must dedupe.
    try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(
            Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()),
            new StringSerializer(),
            new StringSerializer())) {
      producer
          .send(new ProducerRecord<>(OutboxRelay.TOPIC, String.valueOf(transferId), payload))
          .get();
    }
    Thread.sleep(1500); // give the duplicate time to arrive and be deduped
    assertThat(
            jdbc.sql("SELECT count(*) FROM ledger_audit WHERE account_id IN (:a, :b)")
                .param("a", a)
                .param("b", b)
                .query(Long.class)
                .single())
        .isEqualTo(4);

    // balance == source ledger == projection — drift 0 on both axes.
    List<ReconciliationController.AccountReconciliation> report = reconciliation.reconciliation();
    var row = report.stream().filter(r -> r.accountId() == a).findFirst().orElseThrow();
    assertThat(row.balance()).isEqualByComparingTo("90.00");
    assertThat(row.ledgerSum()).isEqualByComparingTo("90.00");
    assertThat(row.projectionSum()).isEqualByComparingTo("90.00");
    assertThat(row.drift()).isEqualByComparingTo(BigDecimal.ZERO);
    assertThat(row.projectionDrift()).isEqualByComparingTo(BigDecimal.ZERO);

    // The eventually-consistent audit view: opening CREDIT then the transfer DEBIT.
    var audit = reconciliation.audit(a);
    assertThat(audit.getBody()).hasSize(2);
    assertThat(audit.getBody().get(0).entry()).isEqualTo("CREDIT");
    assertThat(audit.getBody().get(1).entry()).isEqualTo("DEBIT");
  }
}
