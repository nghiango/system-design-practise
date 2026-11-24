package com.systemdesign.notification.senderworker;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * Per-channel token bucket backed by a single Redis Lua script so refill-and-decrement is atomic
 * across worker replicas. Bucket state lives in one hash {@code <prefix>rate:<channel> = {tokens,
 * ts}} where {@code ts} is the last refill timestamp in milliseconds read from Redis {@code TIME}
 * (no client clock skew). Capacity 10 tokens, refill 10 tokens/second — the exercise's stand-in for
 * a provider quota.
 *
 * <p>A Redis error fails <em>throttled</em>: the notification is requeued rather than sent without
 * a token, because silently bypassing the quota is how real providers ban you.
 */
@Component
class RateLimiter {

  static final int CAPACITY = 10;
  static final double REFILL_PER_SECOND = 10.0;

  /**
   * Returns 1 when a token was consumed, 0 when the bucket is empty. Refills lazily from elapsed
   * time and persists the bucket back with a TTL so idle channels do not leak keys.
   */
  private static final RedisScript<Long> ACQUIRE_SCRIPT =
      new DefaultRedisScript<>(
          """
          local t = redis.call('TIME')
          local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
          local capacity = tonumber(ARGV[1])
          local rate = tonumber(ARGV[2])
          local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
          local tokens = tonumber(data[1]) or capacity
          local ts = tonumber(data[2]) or now
          tokens = math.min(capacity, tokens + (now - ts) / 1000.0 * rate)
          if tokens >= 1 then
            redis.call('HMSET', KEYS[1], 'tokens', tokens - 1, 'ts', now)
            redis.call('PEXPIRE', KEYS[1], 60000)
            return 1
          end
          redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now)
          redis.call('PEXPIRE', KEYS[1], 60000)
          return 0
          """,
          Long.class);

  private static final Logger log = LoggerFactory.getLogger(RateLimiter.class);

  private final StringRedisTemplate redis;
  private final String keyPrefix;

  RateLimiter(
      StringRedisTemplate redis, @Value("${SD_REDIS_KEY_PREFIX:notification:}") String keyPrefix) {
    this.redis = redis;
    this.keyPrefix = keyPrefix;
  }

  /** True when the channel may send now; false when throttled (or Redis is unreachable). */
  boolean tryAcquire(String channel) {
    try {
      Long result =
          redis.execute(
              ACQUIRE_SCRIPT,
              List.of(keyPrefix + "rate:" + channel),
              Integer.toString(CAPACITY),
              Double.toString(REFILL_PER_SECOND));
      return result != null && result == 1L;
    } catch (Exception e) {
      log.warn("rate limiter failed for channel {}: {}", channel, e.toString());
      return false;
    }
  }
}
