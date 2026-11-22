package com.systemdesign.urlshortener.linkapi;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.URISyntaxException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/links")
class LinkController {

  private final LinkService links;
  private final Counter createdCounter;
  private final String shortUrlBase;

  LinkController(
      LinkService links,
      MeterRegistry registry,
      @org.springframework.beans.factory.annotation.Value(
              "${sd.short-url-base:http://localhost:8081}")
          String shortUrlBase) {
    this.links = links;
    this.createdCounter = registry.counter("links.created");
    this.shortUrlBase = shortUrlBase;
  }

  @PostMapping
  ResponseEntity<?> create(@RequestBody CreateRequest request) {
    if (request == null || !isValidHttpUrl(request.url())) {
      return ResponseEntity.badRequest()
          .body(new ErrorResponse("url must be a valid http/https URL"));
    }
    LinkService.Link link = links.create(request.url());
    createdCounter.increment();
    return ResponseEntity.status(HttpStatus.CREATED)
        .body(new CreateResponse(link.code(), shortUrlBase + "/" + link.code(), link.url()));
  }

  @GetMapping("/{code}")
  ResponseEntity<?> stats(@PathVariable String code) {
    return links
        .stats(code)
        .<ResponseEntity<?>>map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  private static boolean isValidHttpUrl(String url) {
    if (url == null || url.isBlank() || url.length() > 4096) {
      return false;
    }
    try {
      URI uri = new URI(url);
      return ("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
          && uri.getHost() != null;
    } catch (URISyntaxException e) {
      return false;
    }
  }

  record CreateRequest(String url) {}

  record CreateResponse(String code, String shortUrl, String url) {}

  record ErrorResponse(String error) {}
}
