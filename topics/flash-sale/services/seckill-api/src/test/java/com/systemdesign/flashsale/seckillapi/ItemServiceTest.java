package com.systemdesign.flashsale.seckillapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.jdbc.core.simple.JdbcClient;

class ItemServiceTest {

  private StringRedisTemplate redis;
  private ValueOperations<String, String> ops;
  private JdbcClient jdbc;
  private ItemService items;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    redis = mock(StringRedisTemplate.class);
    ops = mock(ValueOperations.class);
    jdbc = mock(JdbcClient.class, Mockito.RETURNS_DEEP_STUBS);
    when(redis.opsForValue()).thenReturn(ops);
    items = new ItemService(jdbc, redis, "flash-sale:");
  }

  @Test
  void createSeedsRedisStockKey() {
    when(jdbc.sql(anyString())
            .param(eq("name"), eq("widget"))
            .param(eq("price"), eq(BigDecimal.TEN))
            .param(eq("stock"), eq(100))
            .query(Long.class)
            .single())
        .thenReturn(42L);

    ItemService.Item item = items.create("widget", BigDecimal.TEN, 100);

    assertThat(item.id()).isEqualTo(42L);
    verify(ops).set("flash-sale:stock:42", "100");
  }

  @Test
  void stockKeyUsesTopicPrefix() {
    assertThat(items.stockKey(9L)).isEqualTo("flash-sale:stock:9");
  }
}
