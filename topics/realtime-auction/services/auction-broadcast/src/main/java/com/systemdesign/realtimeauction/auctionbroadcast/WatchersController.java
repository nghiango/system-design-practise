package com.systemdesign.realtimeauction.auctionbroadcast;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Debug aid: how many WebSocket sessions currently watch an auction. */
@RestController
@RequestMapping("/api/auctions")
class WatchersController {

  private final AuctionSocketHandler handler;

  WatchersController(AuctionSocketHandler handler) {
    this.handler = handler;
  }

  @GetMapping("/{id}/watchers")
  WatchersResponse watchers(@PathVariable long id) {
    return new WatchersResponse(id, handler.watchers(id));
  }

  record WatchersResponse(long auctionId, int watchers) {}
}
