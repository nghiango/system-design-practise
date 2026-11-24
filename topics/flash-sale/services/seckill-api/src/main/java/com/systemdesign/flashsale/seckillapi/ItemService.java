package com.systemdesign.flashsale.seckillapi;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Item catalogue. Creating an item writes the row to Postgres and preloads the Redis stock key
 * {@code <prefix>stock:<id>} that the Lua gate decrements. Reads report live stock from Redis,
 * falling back to the Postgres row when Redis is unavailable.
 */
@Service
class ItemService {

  private static final Logger log = LoggerFactory.getLogger(ItemService.class);

  private final JdbcClient jdbc;
  private final StringRedisTemplate redis;
  private final String keyPrefix;

  ItemService(
      JdbcClient jdbc,
      StringRedisTemplate redis,
      @Value("${SD_REDIS_KEY_PREFIX:flash-sale:}") String keyPrefix) {
    this.jdbc = jdbc;
    this.redis = redis;
    this.keyPrefix = keyPrefix;
  }

  String stockKey(long itemId) {
    return keyPrefix + "stock:" + itemId;
  }

  /** Inserts the item and seeds the Redis stock counter. */
  @Transactional
  Item create(String name, BigDecimal price, int stock) {
    long id =
        jdbc.sql(
                "INSERT INTO items (name, price, stock) VALUES (:name, :price, :stock) RETURNING"
                    + " id")
            .param("name", name)
            .param("price", price)
            .param("stock", stock)
            .query(Long.class)
            .single();
    seedStock(id, stock);
    return new Item(id, name, price, stock);
  }

  /** Seeds the Redis stock counter for an item that exists in Postgres. */
  void seedStock(long itemId, int stock) {
    try {
      redis.opsForValue().set(stockKey(itemId), Integer.toString(stock));
    } catch (Exception e) {
      // The sale path re-seeds from DB on a missing key, so a failed preload is not fatal.
      log.warn("redis stock preload failed for item {}: {}", itemId, e.toString());
    }
  }

  /** All items with live stock: the Redis counter when reachable, else the Postgres value. */
  List<ItemView> list() {
    return jdbc
        .sql("SELECT id, name, price, stock FROM items ORDER BY id")
        .query(
            (rs, rowNum) ->
                new Item(
                    rs.getLong("id"),
                    rs.getString("name"),
                    rs.getBigDecimal("price"),
                    rs.getInt("stock")))
        .list()
        .stream()
        .map(item -> new ItemView(item.id(), item.name(), item.price(), liveStock(item)))
        .toList();
  }

  Optional<Item> find(long itemId) {
    return jdbc.sql("SELECT id, name, price, stock FROM items WHERE id = :id")
        .param("id", itemId)
        .query(
            (rs, rowNum) ->
                new Item(
                    rs.getLong("id"),
                    rs.getString("name"),
                    rs.getBigDecimal("price"),
                    rs.getInt("stock")))
        .optional();
  }

  private int liveStock(Item item) {
    try {
      String value = redis.opsForValue().get(stockKey(item.id()));
      if (value != null) {
        return Integer.parseInt(value);
      }
    } catch (Exception e) {
      log.warn("redis stock read failed for item {}: {}", item.id(), e.toString());
    }
    return item.stock();
  }

  record Item(long id, String name, BigDecimal price, int stock) {}

  record ItemView(long id, String name, BigDecimal price, int stock) {}
}
