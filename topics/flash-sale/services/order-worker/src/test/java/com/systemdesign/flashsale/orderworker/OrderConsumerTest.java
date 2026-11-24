package com.systemdesign.flashsale.orderworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.ObjectMapper;

class OrderConsumerTest {

  private JdbcClient jdbc;
  private StringRedisTemplate redis;
  private ValueOperations<String, String> ops;
  private OrderConsumer consumer;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    jdbc = mock(JdbcClient.class, Mockito.RETURNS_DEEP_STUBS);
    redis = mock(StringRedisTemplate.class);
    ops = mock(ValueOperations.class);
    when(redis.opsForValue()).thenReturn(ops);
    consumer = new OrderConsumer(jdbc, redis, new ObjectMapper(), "flash-sale:");
  }

  @Test
  void extractsOrderEvent() {
    OrderConsumer.OrderEvent e = consumer.extract("{\"orderId\":\"o1\",\"itemId\":7}");

    assertThat(e.orderId()).isEqualTo("o1");
    assertThat(e.itemId()).isEqualTo(7L);
  }

  @Test
  void ignoresMalformedMessages() {
    for (String bad :
        new String[] {
          "",
          "not json",
          "{}",
          "{\"orderId\":\"\"}",
          "{\"itemId\":7}",
          "{\"orderId\":\"o\",\"itemId\":\"x\"}",
          "null"
        }) {
      assertThat(consumer.extract(bad)).isNull();
    }
  }

  @Test
  void persistsOrderAndDecrementsDbStock() {
    JdbcClient.StatementSpec spec = mock(JdbcClient.StatementSpec.class);
    when(jdbc.sql(anyString())).thenReturn(spec);
    when(spec.param(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(spec);
    when(spec.update()).thenReturn(1);

    consumer.onOrder("{\"orderId\":\"o1\",\"itemId\":7}");

    verify(jdbc).sql(contains("ON CONFLICT (order_id) DO NOTHING"));
    verify(jdbc).sql(contains("UPDATE items SET stock = stock - 1"));
  }

  @Test
  void duplicateDeliveryStopsAfterInsertConflict() {
    JdbcClient.StatementSpec spec = mock(JdbcClient.StatementSpec.class);
    when(jdbc.sql(anyString())).thenReturn(spec);
    when(spec.param(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(spec);
    when(spec.update()).thenReturn(0); // insert conflict → already persisted

    consumer.onOrder("{\"orderId\":\"o1\",\"itemId\":7}");

    verify(jdbc, never()).sql(contains("UPDATE items"));
    verify(ops, never()).increment(anyString());
  }

  @Test
  void dbDriftCompensatesRedisAndMarksOrderFailed() {
    JdbcClient.StatementSpec spec = mock(JdbcClient.StatementSpec.class);
    when(jdbc.sql(anyString())).thenReturn(spec);
    when(spec.param(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(spec);
    // First update() (insert) = 1, second (items decrement) = 0 → drift.
    when(spec.update()).thenReturn(1).thenReturn(0).thenReturn(1);

    consumer.onOrder("{\"orderId\":\"o1\",\"itemId\":7}");

    verify(jdbc).sql(contains("SET status = 'failed'"));
    verify(ops).increment("flash-sale:stock:7");
  }

  @Test
  void malformedMessageDoesNotTouchDb() {
    consumer.onOrder("garbage");
    verify(jdbc, never()).sql(anyString());
  }
}
