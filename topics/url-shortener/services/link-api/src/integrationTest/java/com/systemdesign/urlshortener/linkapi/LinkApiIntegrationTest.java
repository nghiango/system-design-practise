package com.systemdesign.urlshortener.linkapi;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Runs Flyway migrations and exercises create/stats/click-counting against real Postgres. */
@SpringBootTest(properties = "SD_KAFKA_GROUP_PREFIX=url-shortener.")
@Testcontainers
class LinkApiIntegrationTest {

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

  @DynamicPropertySource
  static void props(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.kafka.bootstrap-servers", () -> "localhost:1");
    registry.add("spring.kafka.listener.auto-startup", () -> "false");
  }

  @Autowired LinkService links;
  @Autowired ClickCounterConsumer consumer;
  @Autowired LinkController controller;

  @Test
  void createAssignsSequentialBase62Code() {
    LinkService.Link first = links.create("https://example.com/one");
    LinkService.Link second = links.create("https://example.com/two");

    assertThat(first.code()).matches("[a-zA-Z0-9]+");
    assertThat(first.code()).isNotEqualTo(second.code());
    assertThat(links.stats(first.code())).isPresent();
  }

  @Test
  void clickConsumerUpsertsCounter() {
    LinkService.Link link = links.create("https://example.com/clicked");

    consumer.onClick("{\"code\":\"" + link.code() + "\"}");
    consumer.onClick("{\"code\":\"" + link.code() + "\"}");
    consumer.onClick("{\"code\":\"" + link.code() + "\"}");

    assertThat(links.stats(link.code()))
        .get()
        .extracting(LinkService.LinkStats::clicks)
        .isEqualTo(3L);
  }

  @Test
  void statsOfUnknownCodeIsEmpty() {
    assertThat(links.stats("nosuchcode")).isEmpty();
  }
}
