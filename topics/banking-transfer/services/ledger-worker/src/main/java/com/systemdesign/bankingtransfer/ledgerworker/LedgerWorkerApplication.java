package com.systemdesign.bankingtransfer.ledgerworker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class LedgerWorkerApplication {

  public static void main(String[] args) {
    SpringApplication.run(LedgerWorkerApplication.class, args);
  }
}
