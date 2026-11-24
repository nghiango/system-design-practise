package com.systemdesign.notification.senderworker;

/**
 * Abstraction over the delivery provider (SMTP, SMS gateway, push, ...). A real client — e.g. a
 * JavaMailSender-backed {@code email} sender or a Twilio {@code sms} sender — implements the same
 * interface; the consumer's retry/DLQ state machine does not change.
 */
interface ChannelSender {

  enum Outcome {
    /** Delivered. */
    SUCCESS,
    /** Retryable error (provider timeout, 5xx, throttling). */
    TRANSIENT,
    /** Non-retryable error (invalid address, rejected target) — straight to DEAD/DLQ. */
    PERMANENT
  }

  Outcome send(String channel, String target, String body);
}
