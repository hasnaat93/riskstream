package com.riskstream;

import static org.assertj.core.api.Assertions.assertThat;

import com.riskstream.model.Alert;
import com.riskstream.model.Payment;
import com.riskstream.simulator.PaymentSimulator;
import com.riskstream.simulator.PaymentSimulator.Labelled;
import com.riskstream.simulator.PaymentSimulator.Scenario;
import com.riskstream.topology.RiskTopology;
import com.riskstream.topology.RuleSettings;
import com.riskstream.topology.StreamMetrics;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

/**
 * Two simulated hours of traffic with fraud incidents mixed in, run through the real
 * topology, and scored against the simulator's labels: how many incidents were caught, and
 * how many alerts fell on honest payments.
 */
class SimulationTest {

    private static final Instant START = Instant.parse("2026-03-02T09:00:00Z");
    private static final int INCIDENTS_PER_SCENARIO = 40;

    private record Outcome(List<Labelled> payments, List<Alert> alerts, StreamMetrics metrics) {
    }

    private static Outcome simulate(long seed, boolean shuffleWithinSeconds) {
        PaymentSimulator simulator = new PaymentSimulator(seed, 4_000);
        Random random = new Random(seed);
        // Incidents first: the simulator gives a victim card some history only if it has none yet,
        // and it must not mistake payments that are still in the future for history.
        List<Labelled> payments = new ArrayList<>();
        for (Scenario scenario : Scenario.values()) {
            if (scenario == Scenario.NORMAL) {
                continue;
            }
            for (int i = 0; i < INCIDENTS_PER_SCENARIO; i++) {
                payments.addAll(simulator.incident(scenario, START.plusSeconds(900 + random.nextInt(6_000))));
            }
        }
        payments.addAll(simulator.normal(START, Duration.ofHours(2), 86_400));    // 12 a second
        payments.sort(Comparator.comparing((Labelled l) -> l.payment().occurredAt()).thenComparing(l -> l.payment().paymentId()));
        if (shuffleWithinSeconds) {
            // deliver out of order: every payment may arrive up to 20 s early or late
            Map<String, Long> arrival = new HashMap<>();
            for (Labelled labelled : payments) {
                arrival.put(labelled.payment().paymentId(),
                        labelled.payment().occurredAt().toEpochMilli() + random.nextInt(40_000) - 20_000);
            }
            payments.sort(Comparator.comparing((Labelled l) -> arrival.get(l.payment().paymentId())));
        }

        StreamMetrics metrics = new StreamMetrics();
        try (TopologyTestDriver driver = new TopologyTestDriver(RiskTopology.build(RuleSettings.defaults(), metrics, false),
                RiskTopology.baseConfig("riskstream-simulation", "unused:9092"))) {
            TestInputTopic<String, Payment> in = driver.createInputTopic(RiskTopology.PAYMENTS, new StringSerializer(),
                    RiskTopology.PAYMENT_SERDE.serializer());
            TestOutputTopic<String, Alert> out = driver.createOutputTopic(RiskTopology.ALERTS, new StringDeserializer(),
                    RiskTopology.ALERT_SERDE.deserializer());
            for (Labelled labelled : payments) {
                in.pipeInput(labelled.payment().cardId(), labelled.payment(), labelled.payment().occurredAt());
            }
            return new Outcome(payments, out.readValuesToList(), metrics);
        }
    }

    /** scenario -> {incidents caught, incidents injected}; plus the alerts raised on honest payments. */
    private record Score(Map<Scenario, int[]> caught, long alertsOnHonestPayments, long honestPayments) {
    }

    private static Score score(Outcome outcome) {
        Map<String, Labelled> byPaymentId = new HashMap<>();
        Map<Scenario, Set<String>> incidents = new EnumMap<>(Scenario.class);
        for (Labelled labelled : outcome.payments()) {
            byPaymentId.put(labelled.payment().paymentId(), labelled);
            if (labelled.incidentId() != null) {
                incidents.computeIfAbsent(labelled.scenario(), s -> new HashSet<>()).add(labelled.incidentId());
            }
        }
        Map<Scenario, Set<String>> caught = new EnumMap<>(Scenario.class);
        long onHonest = 0;
        for (Alert alert : outcome.alerts()) {
            Labelled source = byPaymentId.get(alert.paymentId());
            if (source.scenario() == Scenario.NORMAL) {
                onHonest++;
            } else if (alert.rule().name().equals(source.scenario().expectedRule())) {
                caught.computeIfAbsent(source.scenario(), s -> new HashSet<>()).add(source.incidentId());
            }
        }
        Map<Scenario, int[]> result = new EnumMap<>(Scenario.class);
        incidents.forEach((scenario, ids) -> result.put(scenario,
                new int[] {caught.getOrDefault(scenario, Set.of()).size(), ids.size()}));
        long honest = outcome.payments().stream().filter(l -> l.scenario() == Scenario.NORMAL).count();
        return new Score(result, onHonest, honest);
    }

    private static void print(String title, Outcome outcome, Score score) {
        System.out.println("\n=== " + title + " ===");
        System.out.printf("payments %,d   processed %,d   alerts %,d   suppressed by cooldown %,d   late %,d%n",
                outcome.payments().size(), outcome.metrics().processedCount(), outcome.alerts().size(),
                outcome.metrics().suppressedCount(), outcome.metrics().lateCount());
        score.caught().forEach((scenario, counts) -> System.out.printf("  %-17s caught %d of %d incidents by %s%n",
                scenario, counts[0], counts[1], scenario.expectedRule()));
        System.out.printf("  alerts on honest payments: %d of %,d (%.4f%%)%n", score.alertsOnHonestPayments(),
                score.honestPayments(), 100.0 * score.alertsOnHonestPayments() / score.honestPayments());
    }

    @Test
    void theRulesCatchTheInjectedIncidentsAndRarelyFlagHonestPayments() {
        Outcome outcome = simulate(20260302L, false);
        Score score = score(outcome);
        print("In order", outcome, score);

        assertThat(outcome.metrics().processedCount()).isEqualTo(outcome.payments().size());
        assertThat(outcome.metrics().lateCount()).isZero();
        score.caught().forEach((scenario, counts) -> assertThat(counts[0])
                .as(scenario + ": incidents caught out of " + counts[1]).isGreaterThanOrEqualTo(counts[1] * 9 / 10));
        assertThat(score.caught().get(Scenario.CARD_TESTING)[0]).isEqualTo(INCIDENTS_PER_SCENARIO);
        assertThat(score.caught().get(Scenario.ACCOUNT_TAKEOVER)[0]).isEqualTo(INCIDENTS_PER_SCENARIO);
        // Honest large purchases are in the traffic on purpose, so this is not zero. It must stay small.
        assertThat(100.0 * score.alertsOnHonestPayments() / score.honestPayments()).isLessThan(1.0);
    }

    @Test
    void deliveringThePaymentsOutOfOrderChangesAlmostNothing() {
        Score inOrder = score(simulate(20260302L, false));
        Outcome shuffled = simulate(20260302L, true);
        Score outOfOrder = score(shuffled);
        print("Each payment up to 20 s early or late", shuffled, outOfOrder);

        assertThat(shuffled.metrics().lateCount()).isZero();
        for (Scenario scenario : List.of(Scenario.CARD_TESTING, Scenario.ACCOUNT_TAKEOVER, Scenario.BUST_OUT)) {
            // Windows are judged around the event, so arrival order does not change what a burst
            // looks like. It can change which of two alerts close together falls inside a cooldown,
            // so allow one incident of difference.
            assertThat(outOfOrder.caught().get(scenario)[0]).as(scenario.name())
                    .isBetween(inOrder.caught().get(scenario)[0] - 1, inOrder.caught().get(scenario)[0] + 1);
        }
    }

    @Test
    void theSameSeedGivesTheSameAlerts() {
        List<String> first = simulate(7L, false).alerts().stream().map(Alert::alertId).toList();
        List<String> second = simulate(7L, false).alerts().stream().map(Alert::alertId).toList();
        assertThat(second).isEqualTo(first).isNotEmpty();
    }
}
