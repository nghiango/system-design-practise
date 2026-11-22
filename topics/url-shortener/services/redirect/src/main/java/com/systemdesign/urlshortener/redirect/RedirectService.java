package com.systemdesign.urlshortener.redirect;

import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

/**
 * Cache-aside redirect lookup. Redis key: {@code <topic>:link:<code>} → target URL. Missing links
 * are cached as the {@code NOTFOUND} sentinel for 60s so repeat lookups for bad codes never reach
 * Postgres.
 */
@Service
class RedirectService {

  static final String NOT_FOUND_SENTINEL = "NOTFOUND";
  static final Duration HIT_TTL = Duration.ofHours(1);
  static final Duration MISS_TTL = Duration.ofSeconds(60);

  private static final Logger log = LoggerFactory.getLogger(RedirectService.class);

  private final StringRedisTemplate redis;
  private final JdbcClient jdbc;
  private final String keyPrefix;

  RedirectService(
      StringRedisTemplate redis,
      JdbcClient jdbc,
      @Value("${SD_REDIS_KEY_PREFIX:url-shortener:}") String keyPrefix) {
    this.redis = redis;
    this.jdbc = jdbc;
    this.keyPrefix = keyPrefix;
  }

  /** Returns the target URL for {@code code}, or empty when the link does not exist. */
  Optional<String> resolve(String code) {
    String key = keyPrefix + "link:" + code;
    String cached = getCached(key);
    if (cached != null) {
      return NOT_FOUND_SENTINEL.equals(cached) ? Optional.empty() : Optional.of(cached);
    }

    Optional<String> url =
        jdbc.sql("SELECT url FROM links WHERE code = :code")
            .param("code", code)
            .query(String.class)
            .optional();

    if (url.isPresent()) {
      putCached(key, url.get(), HIT_TTL);
    } else {
      putCached(key, NOT_FOUND_SENTINEL, MISS_TTL);
    }
    return url;
  }

  private String getCached(String key) {
    try {
      return redis.opsForValue().get(key);
    } catch (Exception e) {
      // Redis down must not take redirects down; fall through to Postgres.
      log.warn("redis GET failed for {}: {}", key, e.toString());
      return null;
    }
  }

  private void putCached(String key, String value, Duration ttl) {
    try {
      redis.opsForValue().set(key, value, ttl);
    } catch (Exception e) {
      log.warn("redis SET failed for {}: {}", key, e.toString());
    }
  }
}
