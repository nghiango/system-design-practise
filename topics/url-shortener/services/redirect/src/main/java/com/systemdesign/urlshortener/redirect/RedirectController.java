package com.systemdesign.urlshortener.redirect;

import java.net.URI;
import java.util.Optional;
import java.util.regex.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
class RedirectController {

  private static final Pattern CODE_PATTERN = Pattern.compile("[a-zA-Z0-9]{1,16}");

  private final RedirectService redirects;
  private final ClickEventPublisher clicks;

  RedirectController(RedirectService redirects, ClickEventPublisher clicks) {
    this.redirects = redirects;
    this.clicks = clicks;
  }

  @GetMapping("/{code}")
  ResponseEntity<Void> redirect(@PathVariable String code) {
    if (!CODE_PATTERN.matcher(code).matches()) {
      return ResponseEntity.badRequest().build();
    }
    Optional<String> url = redirects.resolve(code);
    if (url.isEmpty()) {
      return ResponseEntity.notFound().build();
    }
    clicks.publish(code);
    return ResponseEntity.status(HttpStatus.FOUND).location(URI.create(url.get())).build();
  }
}
