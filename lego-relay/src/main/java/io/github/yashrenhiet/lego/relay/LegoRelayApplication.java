package io.github.yashrenhiet.lego.relay;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class LegoRelayApplication {

  public static void main(String[] args) {
    SpringApplication.run(LegoRelayApplication.class, args);
  }
}
