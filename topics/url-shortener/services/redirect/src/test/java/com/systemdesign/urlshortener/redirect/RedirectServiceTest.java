package com.systemdesign.urlshortener.redirect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.jdbc.core.simple.JdbcClient;

class RedirectServiceTest {

  private StringRedisTemplate redis;
  private ValueOperations<String, String> ops;
  private JdbcClient jdbc;
  private RedirectService service;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    redis = mock(StringRedisTemplate.class);
    ops = mock(ValueOperations.class);
    jdbc = mock(JdbcClient.class, org.mockito.Mockito.RETURNS_DEEP_STUBS);
    lenient().when(redis.opsForValue()).thenReturn(ops);
    service = new RedirectService(redis, jdbc, "url-shortener:");
  }

  @Test
  void cacheHitReturnsUrlWithoutDb() {
    when(ops.get("url-shortener:link:abc")).thenReturn("https://example.com");

    assertThat(service.resolve("abc")).contains("https://example.com");
    verify(jdbc, never()).sql(anyString());
  }

  @Test
  void sentinelHitReturnsEmptyWithoutDb() {
    when(ops.get("url-shortener:link:nope")).thenReturn(RedirectService.NOT_FOUND_SENTINEL);

    assertThat(service.resolve("nope")).isEmpty();
    verify(jdbc, never()).sql(anyString());
  }

  @Test
  void missWithDbHitCachesUrlForOneHour() {
    when(ops.get("url-shortener:link:abc")).thenReturn(null);
    when(jdbc.sql(anyString()).param(eq("code"), eq("abc")).query(String.class).optional())
        .thenReturn(Optional.of("https://example.com"));

    assertThat(service.resolve("abc")).contains("https://example.com");
    verify(ops).set("url-shortener:link:abc", "https://example.com", Duration.ofHours(1));
  }

  @Test
  void missWithDbMissCachesSentinelFor60s() {
    when(ops.get("url-shortener:link:nope")).thenReturn(null);
    when(jdbc.sql(anyString()).param(eq("code"), eq("nope")).query(String.class).optional())
        .thenReturn(Optional.empty());

    assertThat(service.resolve("nope")).isEmpty();
    verify(ops)
        .set("url-shortener:link:nope", RedirectService.NOT_FOUND_SENTINEL, Duration.ofSeconds(60));
  }

  @Test
  void redisDownFallsBackToDb() {
    when(ops.get(anyString())).thenThrow(new RuntimeException("connection refused"));
    when(jdbc.sql(anyString()).param(eq("code"), eq("abc")).query(String.class).optional())
        .thenReturn(Optional.of("https://example.com"));

    assertThat(service.resolve("abc")).contains("https://example.com");
  }
}
