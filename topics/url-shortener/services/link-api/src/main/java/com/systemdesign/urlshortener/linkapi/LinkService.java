package com.systemdesign.urlshortener.linkapi;

import java.time.OffsetDateTime;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
class LinkService {

  private final JdbcClient jdbc;

  LinkService(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  /** Reserves an id from the sequence, derives the Base62 code and inserts the link. */
  @Transactional
  Link create(String url) {
    long id = jdbc.sql("SELECT nextval('link_id_seq')").query(Long.class).single();
    String code = Base62.encode(id);
    jdbc.sql("INSERT INTO links (id, code, url) VALUES (:id, :code, :url)")
        .param("id", id)
        .param("code", code)
        .param("url", url)
        .update();
    return new Link(code, url);
  }

  Optional<LinkStats> stats(String code) {
    return jdbc.sql(
            """
            SELECT l.code, l.url, l.created_at, COALESCE(c.clicks, 0) AS clicks
            FROM links l
            LEFT JOIN link_clicks c ON c.code = l.code
            WHERE l.code = :code
            """)
        .param("code", code)
        .query(
            (rs, rowNum) ->
                new LinkStats(
                    rs.getString("code"),
                    rs.getString("url"),
                    rs.getLong("clicks"),
                    rs.getObject("created_at", OffsetDateTime.class)))
        .optional();
  }

  record Link(String code, String url) {}

  record LinkStats(String code, String url, long clicks, OffsetDateTime createdAt) {}
}
