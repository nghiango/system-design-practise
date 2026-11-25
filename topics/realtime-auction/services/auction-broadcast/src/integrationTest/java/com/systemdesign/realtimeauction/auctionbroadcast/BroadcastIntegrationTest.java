package com.systemdesign.realtimeauction.auctionbroadcast;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * End-to-end fan-out evidence: a real WebSocket client on a real port receives the exact update a
 * real Kafka broker delivers to {@code realtime-auction.updates}, and the watchers debug endpoint
 * sees the session.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class BroadcastIntegrationTest {

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
    // The IT produces updates itself; a real consumer group name keeps it isolated.
    registry.add("SD_KAFKA_GROUP_PREFIX", () -> "it-");
  }

  @LocalServerPort int port;
  @Autowired KafkaTemplate<String, String> kafka;

  @Test
  void kafkaUpdateReachesTheWebSocketWatcher() throws Exception {
    long auctionId = 5L;
    CountDownLatch received = new CountDownLatch(1);
    List<String> messages = new CopyOnWriteArrayList<>();

    StandardWebSocketClient client = new StandardWebSocketClient();
    WebSocketSession session =
        client
            .execute(
                new TextWebSocketHandler() {
                  @Override
                  protected void handleTextMessage(WebSocketSession s, TextMessage m) {
                    messages.add(m.getPayload());
                    received.countDown();
                  }
                },
                null,
                URI.create("ws://localhost:" + port + "/ws/auctions/" + auctionId))
            .get(15, TimeUnit.SECONDS);

    // The debug endpoint sees the live session before any update is sent.
    var watchers =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(
                        URI.create(
                            "http://localhost:"
                                + port
                                + "/api/auctions/"
                                + auctionId
                                + "/watchers"))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
    assertThat(watchers.statusCode()).isEqualTo(200);
    assertThat(watchers.body()).contains("\"watchers\":1");

    kafka
        .send(
            "realtime-auction.updates",
            String.valueOf(auctionId),
            "{\"auctionId\":5,\"amount\":41.00,\"bidder\":\"bob\",\"seq\":3}")
        .get(15, TimeUnit.SECONDS);

    assertThat(received.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(messages.get(0)).contains("\"auctionId\":5").contains("\"seq\":3");

    session.close();
  }
}
