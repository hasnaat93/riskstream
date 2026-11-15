package com.riskstream.topology;

import com.riskstream.model.Alert;
import com.riskstream.model.Payment;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * The stream topology: payments in, alerts out.
 *
 * <pre>
 *   payments ──► re-key by card ──► RiskProcessor (state: one record per card) ──► alerts
 * </pre>
 *
 * Built by a pure function with no broker and no Spring, so exactly the same topology runs
 * against Kafka, in the embedded demo runtime, and in the tests.
 */
public final class RiskTopology {

    public static final String PAYMENTS = "payments";
    public static final String ALERTS = "risk-alerts";

    public static final Serde<Payment> PAYMENT_SERDE = JsonSerde.of(Payment.class);
    public static final Serde<Alert> ALERT_SERDE = JsonSerde.of(Alert.class);

    private RiskTopology() {
    }

    public static Topology build(RuleSettings settings, StreamMetrics metrics) {
        return build(settings, metrics, true);
    }

    /**
     * @param changelog back the state stores with changelog topics. On against Kafka, where it
     *                  is what makes state survive a restart; off in the embedded runtime,
     *                  which has no broker to keep them.
     */
    public static Topology build(RuleSettings settings, StreamMetrics metrics, boolean changelog) {
        StreamsBuilder builder = new StreamsBuilder();

        // One record per card, kept in memory and backed by a changelog topic, so a restarted or
        // reassigned task rebuilds its cards from Kafka.
        StoreBuilder<KeyValueStore<String, RiskProcessor.CardState>> cardState = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(RiskProcessor.CARD_STATE),
                Serdes.String(), JsonSerde.of(RiskProcessor.CardState.class));
        builder.addStateStore(changelog ? cardState : cardState.withLoggingDisabled());

        builder.stream(PAYMENTS, Consumed.with(Serdes.String(), PAYMENT_SERDE).withTimestampExtractor(eventTime()))
                // Do not trust the producer's key. All of a card's payments must reach the same
                // task, because that task holds the card's state.
                .filter((key, payment) -> payment != null && payment.cardId() != null)
                .selectKey((key, payment) -> payment.cardId())
                .repartition(Repartitioned.with(Serdes.String(), PAYMENT_SERDE).withName("by-card"))
                .process(() -> new RiskProcessor(settings, metrics), RiskProcessor.CARD_STATE)
                .to(ALERTS, Produced.with(Serdes.String(), ALERT_SERDE));
        return builder.build();
    }

    /** How far ahead of its arrival a payment may claim to have happened: allowance for clock skew. */
    static final Duration MAX_CLOCK_SKEW = Duration.ofMinutes(5);

    /**
     * Event time comes from the payment itself, with two guards. Stream time only moves
     * forwards, and everything older than it by more than the allowed lateness is dropped,
     * so one record with a wrong clock could silence the whole stream.
     *
     * <ul>
     *   <li>A record that carries no payment does not move time at all.</li>
     *   <li>A payment cannot claim to be from the future: its time is capped at the time
     *       Kafka received it, plus an allowance for clock skew.</li>
     * </ul>
     */
    static TimestampExtractor eventTime() {
        return (record, partitionTime) -> {
            if (!(record.value() instanceof Payment payment) || payment.occurredAt() == null) {
                return Math.max(partitionTime, 0);
            }
            long claimed = payment.occurredAt().toEpochMilli();
            long received = record.timestamp();
            return received > 0 ? Math.min(claimed, received + MAX_CLOCK_SKEW.toMillis()) : claimed;
        };
    }

    /** Settings every runtime of this topology shares. */
    public static Properties baseConfig(String applicationId, String bootstrapServers) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // A record that is not valid JSON is logged and skipped. One bad message must not stop the stream.
        config.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        return config;
    }
}
