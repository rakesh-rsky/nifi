package org.apache.nifi.copilot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class NiFiCopilotApplication {
    public static void main(final String[] args) {
        SpringApplication.run(NiFiCopilotApplication.class, args);
    }
}
