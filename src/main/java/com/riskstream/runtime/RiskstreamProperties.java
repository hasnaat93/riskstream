package com.riskstream.runtime;

import com.riskstream.topology.RuleSettings;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("riskstream")
public record RiskstreamProperties(Mode mode, Kafka kafka, Simulator simulator, Rules rules) {

    public enum Mode {
        /** The topology runs in this process on Kafka's own test driver. No broker, no durability. */
        EMBEDDED,
        /** The topology runs as a Kafka Streams application against a Kafka cluster. */
        KAFKA
    }

    public record Kafka(String bootstrapServers, String applicationId, int partitions, int streamThreads) {
    }

    /**
     * @param paymentsPerSecond     ordinary traffic to generate; 0 turns the generator off
     * @param incidentEverySeconds  inject a fraud incident this often; 0 turns that off
     */
    public record Simulator(double paymentsPerSecond, int cards, long seed, int incidentEverySeconds) {
    }

    public record Rules(int velocityMaxPayments, Duration velocityWindow, long spendLimitMinor, Duration spendWindow,
                        Duration geoWindow, double spikeMultiplier, int spikeMinHistory, long spikeMinAmountMinor,
                        Duration cooldown, Duration dedupeRetention, Duration allowedLateness) {

        public RuleSettings toSettings() {
            return new RuleSettings(velocityMaxPayments, velocityWindow, spendLimitMinor, spendWindow, geoWindow,
                    spikeMultiplier, spikeMinHistory, spikeMinAmountMinor, cooldown, dedupeRetention, allowedLateness);
        }
    }
}
