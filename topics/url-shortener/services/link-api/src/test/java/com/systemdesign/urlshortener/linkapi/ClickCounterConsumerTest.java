package com.systemdesign.urlshortener.linkapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.ObjectMapper;

class ClickCounterConsumerTest {

  private JdbcClient jdbc;
  private ClickCounterConsumer consumer;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    jdbc = mock(JdbcClient.class, Mockito.RETURNS_DEEP_STUBS);
    consumer = new ClickCounterConsumer(jdbc, new ObjectMapper());
  }

  @Test
  void extractsCodeFromJson() {
    assertThat(consumer.extractCode("{\"code\":\"bA9\"}")).isEqualTo("bA9");
  }

  @Test
  void ignoresMalformedMessages() {
    for (String bad :
        new String[] {"", "not json", "{}", "{\"code\":123}", "{\"code\":\"\"}", "null"}) {
      assertThat(consumer.extractCode(bad)).isNull();
    }
  }

  @Test
  void upsertsCounterOnClick() {
    JdbcClient.StatementSpec spec = mock(JdbcClient.StatementSpec.class);
    when(jdbc.sql(anyString())).thenReturn(spec);
    when(spec.param("code", "b")).thenReturn(spec);

    consumer.onClick("{\"code\":\"b\"}");

    verify(jdbc).sql(org.mockito.ArgumentMatchers.contains("ON CONFLICT"));
    verify(spec).param("code", "b");
    verify(spec).update();
  }

  @Test
  void doesNotTouchDbForMalformedMessage() {
    consumer.onClick("garbage");
    verify(jdbc, never()).sql(anyString());
  }
}
