package com.systemdesign.notification.senderworker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Component;

/**
 * Deterministic fake provider so the whole pipeline is exercisable without external accounts:
 *
 * <ul>
 *   <li>target containing {@code "fail"} → {@link Outcome#PERMANENT} on every call (e.g. invalid
 *       address — dead immediately, retrying would never help)
 *   <li>target containing {@code "flaky"} → {@link Outcome#TRANSIENT} on the first call for that
 *       target, {@link Outcome#SUCCESS} afterwards (e.g. a provider 5xx that clears — exercises the
 *       retry path while still reaching SENT)
 *   <li>anything else → {@link Outcome#SUCCESS}
 * </ul>
 *
 * The per-target call counter is in-memory simulation state; a real SMTP/SMS client implements
 * {@link ChannelSender} and maps provider error codes onto the same three outcomes.
 */
@Component
class SimulatedChannelSender implements ChannelSender {

  private final ConcurrentMap<String, Integer> callsByTarget = new ConcurrentHashMap<>();

  @Override
  public Outcome send(String channel, String target, String body) {
    if (target.contains("fail")) {
      return Outcome.PERMANENT;
    }
    if (target.contains("flaky")) {
      return callsByTarget.merge(target, 1, Integer::sum) == 1
          ? Outcome.TRANSIENT
          : Outcome.SUCCESS;
    }
    return Outcome.SUCCESS;
  }
}
