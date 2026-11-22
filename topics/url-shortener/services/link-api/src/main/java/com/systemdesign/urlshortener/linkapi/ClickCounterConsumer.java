package com.systemdesign.urlshortener.linkapi;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Counts redirect click events. The Kafka topic name and the consumer group prefix ({@code
 * SD_KAFKA_GROUP_PREFIX}) are injected by the topic chart.
 */
@Component
class ClickCounterConsumer {

  static final String TOPIC = "url-shortener.link.clicked";

  private final JdbcClient jdbc;
  private final ObjectMapper mapper;

  ClickCounterConsumer(JdbcClient jdbc, ObjectMapper mapper) {
    this.jdbc = jdbc;
    this.mapper = mapper;
  }

  @KafkaListener(topics = TOPIC, groupId = "${SD_KAFKA_GROUP_PREFIX:}click-counter")
  void onClick(String message) {
    String code = extractCode(message);
    if (code == null) {
      return;
    }
    increment(code);
  }

  String extractCode(String message) {
    try {
      JsonNode node = mapper.readTree(message);
      JsonNode code = node.get("code");
      return (code != null && code.isString() && !code.asString().isBlank())
          ? code.asString()
          : null;
    } catch (Exception e) {
      return null;
    }
  }

  void increment(String code) {
    jdbc.sql(
            """
            INSERT INTO link_clicks (code, clicks) VALUES (:code, 1)
            ON CONFLICT (code) DO UPDATE SET clicks = link_clicks.clicks + 1
            """)
        .param("code", code)
        .update();
  }
}
