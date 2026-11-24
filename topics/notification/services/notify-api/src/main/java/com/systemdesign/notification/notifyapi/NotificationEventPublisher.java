package com.systemdesign.notification.notifyapi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes {@code {"id","attempt":0}} to {@code notification.requested} after a notification row
 * was inserted. The publish is synchronous: a lost event means the row sits in QUEUED forever, so
 * the caller turns a publish failure into a 503 rather than pretending the submit succeeded.
 */
@Component
class NotificationEventPublisher {

  static final String TOPIC = "notification.requested";

  private static final Logger log = LoggerFactory.getLogger(NotificationEventPublisher.class);

  private final KafkaTemplate<String, String> kafka;

  NotificationEventPublisher(KafkaTemplate<String, String> kafka) {
    this.kafka = kafka;
  }

  /** Returns {@code true} when the event was handed to the broker. */
  boolean publish(long id) {
    try {
      kafka.send(TOPIC, Long.toString(id), "{\"id\":" + id + ",\"attempt\":0}").get();
      return true;
    } catch (Exception e) {
      log.warn("notification event publish failed for {}: {}", id, e.toString());
      return false;
    }
  }
}
