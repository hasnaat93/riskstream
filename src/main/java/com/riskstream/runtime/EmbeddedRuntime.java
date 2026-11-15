package com.riskstream.runtime;

import com.riskstream.model.Alert;
import com.riskstream.model.Payment;
import com.riskstream.topology.RiskTopology;
import com.riskstream.topology.StreamMetrics;
import jakarta.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Runs the topology inside this process on Kafka's {@link TopologyTestDriver}: the same
 * topology, processors and state stores as the Kafka runtime, with no broker.
 *
 * <p>This is what makes a one-container demo possible. It is single-threaded and keeps
 * nothing across a restart, so it demonstrates the rules, not the scaling or fault tolerance
 * that Kafka provides. Those belong to {@link KafkaRuntime}.
 */
@Component
@ConditionalOnProperty(name = "riskstream.mode", havingValue = "embedded", matchIfMissing = true)
public class EmbeddedRuntime implements PaymentBus {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedRuntime.class);
    private static final int DISCARD_EVERY = 200;

    private final TopologyTestDriver driver;
    private final TestInputTopic<String, Payment> payments;
    private final TestOutputTopic<String, Alert> alerts;
    private final Map<String, TestOutputTopic<byte[], byte[]>> internalTopics = new HashMap<>();
    private final AlertFeed feed;
    private int sinceDiscard;

    public EmbeddedRuntime(RiskstreamProperties properties, StreamMetrics metrics, AlertFeed feed) {
        this.feed = feed;
        // No changelog topics: there is no broker to keep them, and nothing reads them here.
        this.driver = new TopologyTestDriver(RiskTopology.build(properties.rules().toSettings(), metrics, false),
                RiskTopology.baseConfig("riskstream-embedded", "embedded:9092"));
        this.payments = driver.createInputTopic(RiskTopology.PAYMENTS, new StringSerializer(),
                RiskTopology.PAYMENT_SERDE.serializer());
        this.alerts = driver.createOutputTopic(RiskTopology.ALERTS, new StringDeserializer(),
                RiskTopology.ALERT_SERDE.deserializer());
        log.info("Embedded runtime started: the topology runs in-process, without a Kafka broker");
    }

    /** The driver is not thread-safe, so one payment is processed at a time. */
    @Override
    public synchronized void publish(Payment payment) {
        payments.pipeInput(payment.cardId(), payment, payment.occurredAt());
        while (!alerts.isEmpty()) {
            feed.publish(alerts.readValue());
        }
        if (++sinceDiscard >= DISCARD_EVERY) {
            discardInternalRecords();
            sinceDiscard = 0;
        }
    }

    /**
     * The driver keeps a copy of every record written to every topic until somebody reads it.
     * Nobody reads the repartition topic's copy, so without this a long-running demo would
     * slowly fill its memory.
     */
    private void discardInternalRecords() {
        for (String topic : driver.producedTopicNames()) {
            if (topic.equals(RiskTopology.ALERTS)) {
                continue;
            }
            internalTopics.computeIfAbsent(topic, name ->
                    driver.createOutputTopic(name, new ByteArrayDeserializer(), new ByteArrayDeserializer()))
                    .readRecordsToList();
        }
    }

    @Override
    public String describe() {
        return "Embedded: the topology runs in-process on Kafka's test driver, without a broker";
    }

    @PreDestroy
    synchronized void stop() {
        driver.close();
    }
}
