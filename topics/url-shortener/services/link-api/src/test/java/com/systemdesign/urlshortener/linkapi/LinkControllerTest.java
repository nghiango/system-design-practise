package com.systemdesign.urlshortener.linkapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.OffsetDateTime;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class LinkControllerTest {

  private LinkService links;
  private SimpleMeterRegistry registry;
  private LinkController controller;

  @BeforeEach
  void setUp() {
    links = mock(LinkService.class);
    registry = new SimpleMeterRegistry();
    controller = new LinkController(links, registry, "http://localhost:8081");
  }

  @Test
  void createsLinkAndReturnsShortUrl() {
    when(links.create("https://example.com/a"))
        .thenReturn(new LinkService.Link("b", "https://example.com/a"));

    ResponseEntity<?> response =
        controller.create(new LinkController.CreateRequest("https://example.com/a"));

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    LinkController.CreateResponse body = (LinkController.CreateResponse) response.getBody();
    assertThat(body.code()).isEqualTo("b");
    assertThat(body.shortUrl()).isEqualTo("http://localhost:8081/b");
    assertThat(body.url()).isEqualTo("https://example.com/a");
    assertThat(registry.get("links.created").counter().count()).isEqualTo(1.0);
  }

  @Test
  void rejectsNonHttpUrls() {
    for (String bad : new String[] {"ftp://x", "javascript:alert(1)", "not a url", "", "  "}) {
      assertThat(controller.create(new LinkController.CreateRequest(bad)).getStatusCode())
          .isEqualTo(HttpStatus.BAD_REQUEST);
    }
    assertThat(registry.get("links.created").counter().count()).isZero();
  }

  @Test
  void returnsStats() {
    when(links.stats("b"))
        .thenReturn(
            Optional.of(
                new LinkService.LinkStats("b", "https://example.com", 7, OffsetDateTime.now())));

    ResponseEntity<?> response = controller.stats("b");

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    LinkService.LinkStats body = (LinkService.LinkStats) response.getBody();
    assertThat(body.clicks()).isEqualTo(7);
  }

  @Test
  void returns404ForUnknownCode() {
    when(links.stats("nope")).thenReturn(Optional.empty());
    assertThat(controller.stats("nope").getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
  }
}
