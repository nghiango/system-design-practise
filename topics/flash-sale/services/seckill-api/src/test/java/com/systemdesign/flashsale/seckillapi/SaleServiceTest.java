package com.systemdesign.flashsale.seckillapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.jdbc.core.simple.JdbcClient;

class SaleServiceTest {

  private StringRedisTemplate redis;
  private ValueOperations<String, String> ops;
  private JdbcClient jdbc;
  private OrderEventPublisher events;
  private ItemService items;
  private SaleService service;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    redis = mock(StringRedisTemplate.class);
    ops = mock(ValueOperations.class);
    jdbc = mock(JdbcClient.class, Mockito.RETURNS_DEEP_STUBS);
    events = mock(OrderEventPublisher.class);
    lenient().when(redis.opsForValue()).thenReturn(ops);
    items = new ItemService(jdbc, redis, "flash-sale:");
    service = new SaleService(redis, items, events);
  }

  @Test
  void decrementAcceptedPublishesEvent() {
    when(redis.execute(
            any(org.springframework.data.redis.core.script.RedisScript.class), anyList()))
        .thenReturn(1L);
    when(events.publish("o1", 7L)).thenReturn(true);

    assertThat(service.buy(7L, "o1")).isEqualTo(SaleService.Result.ACCEPTED);
    verify(events).publish("o1", 7L);
  }

  @Test
  void zeroStockIsSoldOutWithoutEvent() {
    when(redis.execute(
            any(org.springframework.data.redis.core.script.RedisScript.class), anyList()))
        .thenReturn(0L);

    assertThat(service.buy(7L, "o1")).isEqualTo(SaleService.Result.SOLD_OUT);
    verify(events, never()).publish(anyString(), eq(7L));
  }

  @Test
  void redisErrorFailsClosed() {
    when(redis.execute(
            any(org.springframework.data.redis.core.script.RedisScript.class), anyList()))
        .thenThrow(new RuntimeException("connection refused"));

    assertThat(service.buy(7L, "o1")).isEqualTo(SaleService.Result.UNAVAILABLE);
    verify(events, never()).publish(anyString(), eq(7L));
  }

  @Test
  void missingKeySeedsFromDbAndRetries() {
    when(redis.execute(
            any(org.springframework.data.redis.core.script.RedisScript.class), anyList()))
        .thenReturn(-1L)
        .thenReturn(1L);
    when(jdbc.sql(anyString())
            .param(eq("id"), eq(7L))
            .query(any(org.springframework.jdbc.core.RowMapper.class))
            .optional())
        .thenReturn(Optional.of(new ItemService.Item(7L, "widget", BigDecimal.TEN, 5)));
    when(events.publish("o1", 7L)).thenReturn(true);

    assertThat(service.buy(7L, "o1")).isEqualTo(SaleService.Result.ACCEPTED);
    verify(ops).set("flash-sale:stock:7", "5");
  }

  @Test
  void missingKeyUnknownItemIsNotFound() {
    when(redis.execute(
            any(org.springframework.data.redis.core.script.RedisScript.class), anyList()))
        .thenReturn(-1L);
    when(jdbc.sql(anyString())
            .param(eq("id"), eq(7L))
            .query(any(org.springframework.jdbc.core.RowMapper.class))
            .optional())
        .thenReturn(Optional.empty());

    assertThat(service.buy(7L, "o1")).isEqualTo(SaleService.Result.ITEM_NOT_FOUND);
    verify(events, never()).publish(anyString(), eq(7L));
  }

  @Test
  void publishFailureCompensatesStock() {
    when(redis.execute(
            any(org.springframework.data.redis.core.script.RedisScript.class), anyList()))
        .thenReturn(1L);
    when(events.publish("o1", 7L)).thenReturn(false);

    assertThat(service.buy(7L, "o1")).isEqualTo(SaleService.Result.UNAVAILABLE);
    verify(ops).increment("flash-sale:stock:7");
  }
}
