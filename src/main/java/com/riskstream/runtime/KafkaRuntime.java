package com.riskstream.runtime;

import com.riskstream.model.Alert;
import com.riskstream.model.Payment;
import com.riskstream.topology.RiskTopology;
import com.riskstream.topology.StreamMetrics;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Runs the topology as a Kafka Streams application.
 *
 * <ul>
 *   <li><b>Exactly-once processing:</b> reading a payment, updating state and writing the
 *       alert commit together, so a crash and restart neither loses nor repeats an alert.</li>
 *   <li><b>Scaling:</b> start more instances with the same application id and Kafka shares
 *       the partitions between them. All of a card's payments are on one partition, so its
 *       state is always in one place.</li>
 *   <li><b>Recovery:</b> state stores are backed by changelog topics and rebuilt from them.</li>
 * </ul>
 */
@Component
@ConditionalOnProperty(name = "riskstream.mode", havingValue = "kafka")
public class KafkaRuntime implements PaymentBus {

    private static final Logger log = LoggerFactory.getLogger(KafkaRuntime.class);

    private final RiskstreamProperties.Kafka settings;
    private final AlertFeed feed;
    private final KafkaStreams streams;
    private final KafkaProducer<String, Payment> producer;
    private final KafkaConsumer<String, Alert> alertConsumer;
    private final Thread alertThread;
    private volatile boolean running = true;

    public KafkaRuntime(RiskstreamProperties properties, StreamMetrics metrics, AlertFeed feed) {
        this.settings = properties.kafka();
        this.feed = feed;
        createTopics();

        Properties config = RiskTopology.baseConfig(settings.applicationId(), settings.bootstrapServers());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, settings.streamThreads());
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);       // alerts become visible within this
        config.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        this.streams = new KafkaStreams(RiskTopology.build(properties.rules().toSettings(), metrics, true), config);
        // A failure in one stream thread replaces that thread instead of taking the application down.
        streams.setUncaughtExceptionHandler(error -> {
            log.error("Stream thread failed; replacing it", error);
            return StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        this.producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers(),
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.LINGER_MS_CONFIG, 5),
                new StringSerializer(), RiskTopology.PAYMENT_SERDE.serializer());

        // The console tails alerts from "now": its own consumer group, committed transactions only.
        this.alertConsumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, settings.applicationId() + "-console-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false),
                new StringDeserializer(), RiskTopology.ALERT_SERDE.deserializer());
        this.alertThread = new Thread(this::tailAlerts, "alert-tail");
        this.alertThread.setDaemon(true);
    }

    private void createTopics() {
        try (Admin admin = Admin.create(Map.<String, Object>of("bootstrap.servers", settings.bootstrapServers()))) {
            for (String topic : List.of(RiskTopology.PAYMENTS, RiskTopology.ALERTS)) {
                try {
                    admin.createTopics(List.of(new NewTopic(topic, settings.partitions(), (short) -1)))
                            .all().get(30, TimeUnit.SECONDS);
                    log.info("Created topic {} with {} partitions", topic, settings.partitions());
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof TopicExistsException)) {
                        throw new IllegalStateException("Could not create topic " + topic, e.getCause());
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while creating topics", e);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("Kafka at " + settings.bootstrapServers() + " did not answer in time", e);
        }
    }

    @PostConstruct
    void start() {
        alertConsumer.subscribe(List.of(RiskTopology.ALERTS));
        alertThread.start();
        streams.start();
        log.info("Kafka runtime started against {} as application '{}'", settings.bootstrapServers(),
                settings.applicationId());
    }

    private void tailAlerts() {
        try {
            while (running) {
                for (ConsumerRecord<String, Alert> record : alertConsumer.poll(Duration.ofMillis(250))) {
                    if (record.value() != null) {
                        feed.publish(record.value());
                    }
                }
            }
        } catch (WakeupException stopping) {
            // close() woke the poll
        } finally {
            alertConsumer.close();
        }
    }

    @Override
    public void publish(Payment payment) {
        producer.send(new ProducerRecord<>(RiskTopology.PAYMENTS, null, payment.occurredAt().toEpochMilli(),
                payment.cardId(), payment));
    }

    public KafkaStreams.State state() {
        return streams.state();
    }

    @Override
    public String describe() {
        return "Kafka Streams on " + settings.bootstrapServers() + ", exactly-once, state " + streams.state();
    }

    @PreDestroy
    void stop() {
        running = false;
        alertConsumer.wakeup();
        producer.close(Duration.ofSeconds(5));
        streams.close(Duration.ofSeconds(10));
    }
}
