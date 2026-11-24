package com.systemdesign.notification.senderworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

class RateLimiterTest {

  private StringRedisTemplate redis;
  private RateLimiter limiter;

  @BeforeEach
  void setUp() {
    redis = mock(StringRedisTemplate.class);
    limiter = new RateLimiter(redis, "notification:");
  }

  @Test
  @SuppressWarnings("unchecked")
  void tokenAvailableAcquires() {
    when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString())).thenReturn(1L);

    assertThat(limiter.tryAcquire("email")).isTrue();
  }

  @Test
  @SuppressWarnings("unchecked")
  void emptyBucketThrottles() {
    when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString())).thenReturn(0L);

    assertThat(limiter.tryAcquire("email")).isFalse();
  }

  @Test
  @SuppressWarnings("unchecked")
  void redisErrorFailsThrottled() {
    when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString()))
        .thenThrow(new RuntimeException("connection refused"));

    assertThat(limiter.tryAcquire("email")).isFalse();
  }
}
