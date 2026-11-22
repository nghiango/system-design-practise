package com.systemdesign.urlshortener.redirect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes {"code": "..."} click events. Publishing is best-effort: a Kafka failure must never
 * break a redirect.
 */
@Component
class ClickEventPublisher {

  static final String TOPIC = "url-shortener.link.clicked";

  private static final Logger log = LoggerFactory.getLogger(ClickEventPublisher.class);

  private final KafkaTemplate<String, String> kafka;

  ClickEventPublisher(KafkaTemplate<String, String> kafka) {
    this.kafka = kafka;
  }

  void publish(String code) {
    try {
      kafka
          .send(TOPIC, code, "{\"code\":\"" + code + "\"}")
          .whenComplete(
              (result, error) -> {
                if (error != null) {
                  log.warn("click event publish failed for {}: {}", code, error.toString());
                }
              });
    } catch (Exception e) {
      log.warn("click event publish failed for {}: {}", code, e.toString());
    }
  }
}
