package com.riskstream.simulator;

import com.riskstream.model.Alert;
import com.riskstream.model.Payment;
import com.riskstream.runtime.AlertFeed;
import com.riskstream.runtime.PaymentBus;
import com.riskstream.runtime.RiskstreamProperties;
import com.riskstream.simulator.PaymentSimulator.Labelled;
import com.riskstream.simulator.PaymentSimulator.Scenario;
import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Feeds the stream with simulated traffic and keeps score: for every fraud incident it
 * injects it remembers which rule should catch it, and watches the alerts to see whether
 * one did. The score is the only place the simulator's labels are used.
 */
@Component
public class TrafficGenerator {

    private static final long TICK_MS = 250;
    private static final Scenario[] ROTATION = {
        Scenario.CARD_TESTING, Scenario.AMOUNT_SPIKE, Scenario.ACCOUNT_TAKEOVER, Scenario.BUST_OUT};

    /** An injected incident waiting to be detected: card and the rule expected to fire. */
    private record Expected(Scenario scenario, String cardId, String rule) {
    }

    private final PaymentBus bus;
    private final PaymentSimulator simulator;
    private final RiskstreamProperties.Simulator settings;
    private final Clock clock;
    private final Map<String, Expected> undetected = new ConcurrentHashMap<>();
    private final Map<Scenario, AtomicLong> injected = new EnumMap<>(Scenario.class);
    private final Map<Scenario, AtomicLong> detected = new EnumMap<>(Scenario.class);
    private final AtomicLong published = new AtomicLong();
    private double owed;
    private long ticksSinceIncident;
    private int rotation;

    public TrafficGenerator(PaymentBus bus, RiskstreamProperties properties, AlertFeed feed, Clock clock) {
        this.bus = bus;
        this.settings = properties.simulator();
        // ids are unique to this run of the process, so a restart never reuses one
        this.simulator = new PaymentSimulator(settings.seed(), settings.cards(),
                "pay-" + Long.toString(clock.millis() / 1000, 36) + "-");
        this.clock = clock;
        for (Scenario scenario : Scenario.values()) {
            injected.put(scenario, new AtomicLong());
            detected.put(scenario, new AtomicLong());
        }
        feed.observe(this::onAlert);
    }

    @Scheduled(fixedRate = TICK_MS)
    void tick() {
        if (settings.paymentsPerSecond() > 0) {
            owed += settings.paymentsPerSecond() * TICK_MS / 1000.0;
            Instant now = clock.instant();
            synchronized (simulator) {
                while (owed >= 1) {
                    owed -= 1;
                    send(simulator.normal(now).payment());
                }
            }
        }
        if (settings.incidentEverySeconds() > 0 && ++ticksSinceIncident * TICK_MS >= settings.incidentEverySeconds() * 1000L) {
            ticksSinceIncident = 0;
            inject(ROTATION[rotation++ % ROTATION.length]);
        }
    }

    /**
     * Plays one fraud incident into the stream. The incident is dated so that it ends now:
     * its payments carry the times they would have had, and arrive together, which is also
     * what a consumer catching up after a pause looks like.
     *
     * @return the number of payments published
     */
    public int inject(Scenario scenario) {
        List<Labelled> payments;
        synchronized (simulator) {
            payments = simulator.incident(scenario, clock.instant().minus(PaymentSimulator.longestSpan(scenario)));
        }
        Labelled marked = payments.stream().filter(p -> p.incidentId() != null).findFirst().orElseThrow();
        undetected.put(marked.incidentId(), new Expected(scenario, marked.payment().cardId(), scenario.expectedRule()));
        injected.get(scenario).incrementAndGet();
        payments.stream().map(Labelled::payment).sorted(Comparator.comparing(Payment::occurredAt)).forEach(this::send);
        return payments.size();
    }

    public void send(Payment payment) {
        bus.publish(payment);
        published.incrementAndGet();
    }

    private void onAlert(Alert alert) {
        undetected.entrySet().removeIf(entry -> {
            Expected expected = entry.getValue();
            boolean caught = expected.cardId().equals(alert.cardId()) && expected.rule().equals(alert.rule().name());
            if (caught) {
                detected.get(expected.scenario()).incrementAndGet();
            }
            return caught;
        });
    }

    public long publishedCount() {
        return published.get();
    }

    public long injectedCount(Scenario scenario) {
        return injected.get(scenario).get();
    }

    public long detectedCount(Scenario scenario) {
        return detected.get(scenario).get();
    }
}
