package com.riskstream;

import com.riskstream.topology.StreamMetrics;
import java.time.Clock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class RiskstreamApplication {

    public static void main(String[] args) {
        SpringApplication.run(RiskstreamApplication.class, args);
    }

    /** Counters shared between the stream processor and the API. */
    @Bean
    StreamMetrics streamMetrics() {
        return new StreamMetrics();
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
