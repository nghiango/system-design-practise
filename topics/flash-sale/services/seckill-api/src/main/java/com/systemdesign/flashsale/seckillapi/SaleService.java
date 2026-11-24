package com.systemdesign.flashsale.seckillapi;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

/**
 * The atomic buy gate. A single Lua script reads and decrements the stock counter in one Redis
 * round trip, so N concurrent buyers can never take more than the seeded stock. The gate fails
 * <em>closed</em>: any Redis error maps to {@link Result#UNAVAILABLE} (503) rather than risking an
 * oversell.
 *
 * <p>A missing stock key (service restarted after items were created, or preload failed) is seeded
 * once from Postgres and the script retried once. An item that does not exist in Postgres maps to
 * {@link Result#ITEM_NOT_FOUND}.
 */
@Service
class SaleService {

  enum Result {
    ACCEPTED,
    SOLD_OUT,
    ITEM_NOT_FOUND,
    UNAVAILABLE
  }

  /**
   * Returns 1 = decremented, 0 = counter reached zero, -1 = key absent (caller may seed from DB and
   * retry).
   */
  private static final RedisScript<Long> DECR_SCRIPT =
      new DefaultRedisScript<>(
          """
          local s = tonumber(redis.call('GET', KEYS[1]) or '-1')
          if s == -1 then return -1 end
          if s > 0 then redis.call('DECR', KEYS[1]); return 1 end
          return 0
          """,
          Long.class);

  private static final Logger log = LoggerFactory.getLogger(SaleService.class);

  private final StringRedisTemplate redis;
  private final ItemService items;
  private final OrderEventPublisher events;

  SaleService(StringRedisTemplate redis, ItemService items, OrderEventPublisher events) {
    this.redis = redis;
    this.items = items;
    this.events = events;
  }

  /**
   * Attempts to reserve one unit of {@code itemId} for {@code orderId}. Never oversells: it either
   * decrements the counter and emits the order event, or returns a rejection.
   */
  Result buy(long itemId, String orderId) {
    Long outcome = tryDecr(items.stockKey(itemId));
    if (outcome == null) {
      return Result.UNAVAILABLE;
    }
    if (outcome == -1) {
      // Stock key missing: seed once from Postgres if the item exists, then retry once.
      var item = items.find(itemId);
      if (item.isEmpty()) {
        return Result.ITEM_NOT_FOUND;
      }
      items.seedStock(itemId, item.get().stock());
      outcome = tryDecr(items.stockKey(itemId));
      if (outcome == null) {
        return Result.UNAVAILABLE;
      }
      if (outcome == -1) {
        log.warn("stock key {} still missing after reseed", items.stockKey(itemId));
        return Result.UNAVAILABLE;
      }
    }
    if (outcome == 0) {
      return Result.SOLD_OUT;
    }

    if (!events.publish(orderId, itemId)) {
      // Compensate: give the reserved unit back so the item is not lost from sellable stock.
      compensate(items.stockKey(itemId));
      return Result.UNAVAILABLE;
    }
    return Result.ACCEPTED;
  }

  private Long tryDecr(String key) {
    try {
      return redis.execute(DECR_SCRIPT, List.of(key));
    } catch (Exception e) {
      // Fail closed: never answer "accepted" or "sold out" on infra failure.
      log.warn("redis lua gate failed for {}: {}", key, e.toString());
      return null;
    }
  }

  private void compensate(String key) {
    try {
      redis.opsForValue().increment(key);
    } catch (Exception e) {
      log.error("compensation INCR failed for {} — stock drift possible: {}", key, e.toString());
    }
  }
}
