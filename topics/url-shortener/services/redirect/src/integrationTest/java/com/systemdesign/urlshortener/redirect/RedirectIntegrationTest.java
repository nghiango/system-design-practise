package com.systemdesign.urlshortener.redirect;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Cache-aside behaviour against real Postgres + Redis. No Kafka broker is started: the click
 * producer fails, which also proves redirects survive a dead broker.
 */
@SpringBootTest(properties = "SD_REDIS_KEY_PREFIX=url-shortener:")
@Testcontainers
class RedirectIntegrationTest {

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

  @Container
  static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

  @DynamicPropertySource
  static void props(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.data.redis.host", REDIS::getHost);
    registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    registry.add("spring.kafka.bootstrap-servers", () -> "localhost:1");
  }

  @Autowired RedirectService redirects;
  @Autowired ClickEventPublisher clicks;
  @Autowired StringRedisTemplate redis;
  @Autowired JdbcClient jdbc;

  private void createSchema() {
    jdbc.sql(
            "CREATE TABLE IF NOT EXISTS links ("
                + "id bigint PRIMARY KEY, code text NOT NULL UNIQUE, url text NOT NULL,"
                + " created_at timestamptz NOT NULL DEFAULT now())")
        .update();
  }

  @Test
  void missThenHitThenNegativeCache() {
    createSchema();
    jdbc.sql("INSERT INTO links (id, code, url) VALUES (1, 'it1', 'https://example.com/it')")
        .param("id", 1)
        .param("code", "it1")
        .param("url", "https://example.com/it")
        .update();

    // First resolve: DB hit, cached with 1h TTL.
    Optional<String> url = redirects.resolve("it1");
    assertThat(url).contains("https://example.com/it");
    String cached = redis.opsForValue().get("url-shortener:link:it1");
    assertThat(cached).isEqualTo("https://example.com/it");

    // Second resolve served from cache (delete the row to prove it).
    jdbc.sql("DELETE FROM links WHERE code = :code").param("code", "it1").update();
    assertThat(redirects.resolve("it1")).contains("https://example.com/it");

    // Unknown code resolves empty and gets a sentinel.
    assertThat(redirects.resolve("gone")).isEmpty();
    assertThat(redis.opsForValue().get("url-shortener:link:gone"))
        .isEqualTo(RedirectService.NOT_FOUND_SENTINEL);
    Long ttl = redis.getExpire("url-shortener:link:gone");
    assertThat(ttl).isNotNull().isBetween(1L, 60L);
  }

  @Test
  void clickPublishDoesNotThrowWithoutBroker() {
    org.assertj.core.api.Assertions.assertThatCode(() -> clicks.publish("it1"))
        .doesNotThrowAnyException();
  }
}
