package com.riskstream;

import static org.assertj.core.api.Assertions.assertThat;

import com.riskstream.model.Alert;
import com.riskstream.model.Alert.Rule;
import com.riskstream.model.Payment;
import com.riskstream.topology.RiskTopology;
import com.riskstream.topology.RuleSettings;
import com.riskstream.topology.StreamMetrics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The real topology, driven record by record with no broker. Time here is whatever the
 * payments say it is, so every case is exact and repeatable.
 */
class RiskTopologyTest {

    private static final Instant T0 = Instant.parse("2026-03-02T10:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, Payment> payments;
    private TestOutputTopic<String, Alert> alerts;
    private StreamMetrics metrics;
    private int sequence;

    @BeforeEach
    void start() {
        metrics = new StreamMetrics();
        driver = new TopologyTestDriver(RiskTopology.build(RuleSettings.defaults(), metrics),
                RiskTopology.baseConfig("riskstream-test", "unused:9092"));
        payments = driver.createInputTopic(RiskTopology.PAYMENTS, new StringSerializer(),
                RiskTopology.PAYMENT_SERDE.serializer());
        alerts = driver.createOutputTopic(RiskTopology.ALERTS, new StringDeserializer(),
                RiskTopology.ALERT_SERDE.deserializer());
    }

    @AfterEach
    void stop() {
        driver.close();
    }

    private Payment payment(String card, long amountMinor, String country, Duration afterStart) {
        return new Payment("p-" + (++sequence), card, "merchant-1", amountMinor, "USD", country, "ECOMMERCE",
                T0.plus(afterStart));
    }

    private Payment pay(String card, long amountMinor, Duration afterStart) {
        return send(payment(card, amountMinor, "US", afterStart));
    }

    private Payment send(Payment payment) {
        payments.pipeInput(payment.cardId(), payment);
        return payment;
    }

    private List<Alert> fired() {
        return alerts.readValuesToList();
    }

    private static Duration s(long seconds) {
        return Duration.ofSeconds(seconds);
    }

    // ------------------------------------------------------------ velocity

    @Test
    void theSixthPaymentInsideAMinuteRaisesAVelocityAlert() {
        for (int i = 0; i < 5; i++) {
            pay("card-a", 500, s(i * 10));
        }
        assertThat(fired()).as("five is within the limit").isEmpty();

        Payment sixth = pay("card-a", 500, s(55));
        List<Alert> raised = fired();
        assertThat(raised).hasSize(1);
        Alert alert = raised.get(0);
        assertThat(alert.rule()).isEqualTo(Rule.VELOCITY);
        assertThat(alert.severity()).isEqualTo(Alert.Severity.HIGH);
        assertThat(alert.cardId()).isEqualTo("card-a");
        assertThat(alert.paymentId()).isEqualTo(sixth.paymentId());
        assertThat(alert.eventTime()).isEqualTo(T0.plusSeconds(55));
        assertThat(alert.summary()).isEqualTo("6 payments on one card within 60 s");
        assertThat(alert.evidence()).containsEntry("payments", 6).containsEntry("limit", 5)
                .containsEntry("firstAt", "2026-03-02T10:00:00Z").containsEntry("lastAt", "2026-03-02T10:00:55Z");
    }

    @Test
    void sixPaymentsSpreadOverMoreThanAMinuteDoNotAlert() {
        for (int i = 0; i < 6; i++) {
            pay("card-a", 500, s(i * 12));       // the first and the sixth are exactly 60 s apart
        }
        assertThat(fired()).isEmpty();
    }

    @Test
    void cardsAreJudgedSeparately() {
        for (int i = 0; i < 12; i++) {
            pay("card-" + (i % 3), 500, s(i));  // four payments each on three cards
        }
        assertThat(fired()).isEmpty();
    }

    @Test
    void aBurstThatArrivesOutOfOrderIsStillCaughtExactlyOnce() {
        List<Payment> burst = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            burst.add(payment("card-a", 300, "US", s(i * 5)));
        }
        Collections.shuffle(burst, new Random(11));
        burst.forEach(this::send);

        List<Alert> raised = fired();
        assertThat(raised).extracting(Alert::rule).containsExactly(Rule.VELOCITY);
        assertThat(metrics.suppressedCount()).as("later payments of the same burst").isEqualTo(3);
    }

    @Test
    void aRuleStaysQuietForACardDuringItsCooldownThenCanFireAgain() {
        for (int i = 0; i < 12; i++) {
            pay("card-a", 500, s(i * 4));
        }
        assertThat(fired()).hasSize(1);

        for (int i = 0; i < 8; i++) {
            pay("card-a", 500, Duration.ofMinutes(4).plusSeconds(i * 4));       // inside the 5 minute cooldown
        }
        assertThat(fired()).isEmpty();

        for (int i = 0; i < 8; i++) {
            pay("card-a", 500, Duration.ofMinutes(12).plusSeconds(i * 4));
        }
        assertThat(fired()).extracting(Alert::rule).containsExactly(Rule.VELOCITY);
    }

    // ---------------------------------------------------------- duplicates

    @Test
    void aPaymentDeliveredTwiceCountsOnce() {
        List<Payment> sent = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            sent.add(pay("card-a", 500, s(i * 5)));
        }
        sent.forEach(this::send);                // the producer retries all five
        send(sent.get(0));

        assertThat(fired()).as("ten deliveries, five payments: no burst").isEmpty();
        assertThat(metrics.processedCount()).isEqualTo(5);
        assertThat(metrics.duplicateCount()).isEqualTo(6);
    }

    @Test
    void theSamePaymentAndRuleAlwaysProduceTheSameAlertId() {
        Alert first = Alert.of(Rule.VELOCITY, Alert.Severity.HIGH, payment("card-a", 1, "US", s(0)), "x", java.util.Map.of());
        sequence--;
        Alert again = Alert.of(Rule.VELOCITY, Alert.Severity.HIGH, payment("card-a", 1, "US", s(0)), "x", java.util.Map.of());
        sequence--;
        Alert other = Alert.of(Rule.GEO_JUMP, Alert.Severity.HIGH, payment("card-a", 1, "US", s(0)), "x", java.util.Map.of());
        assertThat(again.alertId()).isEqualTo(first.alertId()).isNotEqualTo(other.alertId());
    }

    // --------------------------------------------------------------- spend

    @Test
    void spendOverTheLimitInsideTheWindowAlerts() {
        pay("card-a", 90_000, s(0));
        pay("card-a", 90_000, Duration.ofMinutes(3));
        assertThat(fired()).as("1,800.00 so far").isEmpty();

        pay("card-a", 80_000, Duration.ofMinutes(6));
        List<Alert> raised = fired();
        assertThat(raised).extracting(Alert::rule).containsExactly(Rule.SPEND_LIMIT);
        assertThat(raised.get(0).summary()).isEqualTo("Spend of 2600.00 within 10 min, over the limit of 2500.00");
        assertThat(raised.get(0).evidence()).containsEntry("totalMinor", 260_000).containsEntry("payments", 3);
    }

    @Test
    void theSameSpendSpreadOverALongerPeriodDoesNotAlert() {
        pay("card-a", 90_000, s(0));
        pay("card-a", 90_000, Duration.ofMinutes(6));
        pay("card-a", 80_000, Duration.ofMinutes(12));
        assertThat(fired()).isEmpty();
    }

    // ----------------------------------------------------------------- geo

    @Test
    void aCardUsedInTwoCountriesWithinHalfAnHourAlerts() {
        send(payment("card-a", 2_000, "PK", s(0)));
        Payment abroad = send(payment("card-a", 2_000, "BR", Duration.ofMinutes(12)));

        List<Alert> raised = fired();
        assertThat(raised).extracting(Alert::rule).containsExactly(Rule.GEO_JUMP);
        assertThat(raised.get(0).paymentId()).isEqualTo(abroad.paymentId());
        assertThat(raised.get(0).summary()).isEqualTo("Used in PK and BR 12 min apart");
    }

    @Test
    void aSecondCountryAfterMoreThanHalfAnHourIsOrdinaryTravel() {
        send(payment("card-a", 2_000, "PK", s(0)));
        send(payment("card-a", 2_000, "AE", Duration.ofMinutes(31)));
        send(payment("card-a", 2_000, "AE", Duration.ofMinutes(40)));
        assertThat(fired()).isEmpty();
    }

    @Test
    void theGeoRuleWorksWhicheverOfTheTwoPaymentsArrivesFirst() {
        send(payment("card-a", 2_000, "BR", Duration.ofMinutes(12)));
        assertThat(fired()).isEmpty();
        send(payment("card-a", 2_000, "PK", s(0)));      // the earlier payment, delivered late
        assertThat(fired()).extracting(Alert::rule).containsExactly(Rule.GEO_JUMP);
    }

    // --------------------------------------------------------------- spike

    @Test
    void aPaymentFarAboveTheCardsAverageAlertsOnceThereIsEnoughHistory() {
        for (int i = 0; i < 5; i++) {
            pay("card-a", 2_000, Duration.ofMinutes(i * 2));
        }
        pay("card-a", 15_000, Duration.ofMinutes(11));        // 7.5 times the average: under the multiplier
        assertThat(fired()).isEmpty();

        pay("card-a", 40_000, Duration.ofMinutes(13));        // average is now about 41.67
        List<Alert> raised = fired();
        assertThat(raised).extracting(Alert::rule).containsExactly(Rule.AMOUNT_SPIKE);
        assertThat(raised.get(0).summary()).isEqualTo("400.00 against a card average of 41.66");
        assertThat(raised.get(0).evidence()).containsEntry("earlierPayments", 6).containsEntry("timesAverage", 9.6);
    }

    @Test
    void aLargeFirstPaymentIsNotASpikeBecauseThereIsNothingToCompareItWith() {
        pay("card-new", 90_000, s(0));
        pay("card-new", 500, Duration.ofMinutes(15));
        assertThat(fired()).isEmpty();
    }

    @Test
    void aLargeMultipleOfATinyAverageIsIgnoredBelowTheMinimumAmount() {
        for (int i = 0; i < 6; i++) {
            pay("card-a", 100, Duration.ofMinutes(i * 2));
        }
        pay("card-a", 5_000, Duration.ofMinutes(13));          // 50 times the average, but only 50.00
        assertThat(fired()).isEmpty();
    }

    // ------------------------------------------------- lateness, bad input

    @Test
    void aPaymentOlderThanTheAllowedLatenessIsDroppedAndCounted() {
        pay("card-a", 500, Duration.ofMinutes(60));
        pay("card-b", 500, Duration.ofMinutes(29).plusSeconds(59));       // 30 min 1 s behind: too late
        pay("card-c", 500, Duration.ofMinutes(31));                        // 29 min behind: accepted

        assertThat(metrics.lateCount()).isEqualTo(1);
        assertThat(metrics.processedCount()).isEqualTo(2);
    }

    @Test
    void aRecordThatIsNotValidJsonIsSkippedAndTheStreamCarriesOn() {
        TestInputTopic<String, byte[]> raw = driver.createInputTopic(RiskTopology.PAYMENTS, new StringSerializer(),
                new ByteArraySerializer());
        raw.pipeInput("card-a", "{ not json".getBytes(StandardCharsets.UTF_8));
        raw.pipeInput("card-a", "{\"paymentId\":\"x\"}".getBytes(StandardCharsets.UTF_8));     // valid JSON, no card
        raw.pipeInput("card-a", null);

        for (int i = 0; i < 6; i++) {
            pay("card-a", 500, s(i * 5));
        }
        assertThat(fired()).extracting(Alert::rule).containsExactly(Rule.VELOCITY);
        assertThat(metrics.processedCount()).isEqualTo(6);
    }

    @Test
    void aPaymentClaimingToBeFromTheFutureCannotMakeRealPaymentsLookLate() {
        // Received now, but dated ten years ahead. Unchecked, it would drag stream time forward
        // and every genuine payment after it would be dropped as late.
        Payment fromTheFuture = payment("card-z", 500, "US", Duration.ofDays(3650));
        payments.pipeInput(fromTheFuture.cardId(), fromTheFuture, T0);

        for (int i = 0; i < 6; i++) {
            Payment p = payment("card-a", 500, "US", s(i * 5));
            payments.pipeInput(p.cardId(), p, p.occurredAt());
        }
        assertThat(metrics.lateCount()).isZero();
        assertThat(fired()).extracting(Alert::rule).containsExactly(Rule.VELOCITY);
    }

    @Test
    void paymentsAreGroupedByCardWhateverKeyTheProducerUsed() {
        for (int i = 0; i < 6; i++) {
            Payment p = payment("card-a", 500, "US", s(i * 5));
            payments.pipeInput(i % 2 == 0 ? null : "some-other-key-" + i, p);
        }
        List<Alert> raised = fired();
        assertThat(raised).hasSize(1);
        assertThat(alerts.isEmpty()).isTrue();
        assertThat(raised.get(0).cardId()).isEqualTo("card-a");
    }

    @Test
    void oneBurstCanTripMoreThanOneRule() {
        for (int i = 0; i < 6; i++) {
            pay("card-a", 50_000, s(i * 8));     // six payments of 500.00 in under a minute: 3,000.00
        }
        assertThat(fired()).extracting(Alert::rule).containsExactlyInAnyOrder(Rule.VELOCITY, Rule.SPEND_LIMIT);
    }
}
