package com.systemdesign.realtimeauction.auctionbroadcast;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/** Exposes the bid stream at {@code /ws/auctions/{auctionId}} (decision 002). */
@Configuration
@EnableWebSocket
class WebSocketConfig implements WebSocketConfigurer {

  @Bean
  AuctionSocketHandler auctionSocketHandler() {
    return new AuctionSocketHandler();
  }

  @Override
  public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
    registry.addHandler(auctionSocketHandler(), "/ws/auctions/*").setAllowedOriginPatterns("*");
  }
}
