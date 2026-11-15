package com.riskstream.web;

import com.riskstream.common.ApiException;
import com.riskstream.model.Alert;
import com.riskstream.model.Payment;
import com.riskstream.runtime.AlertFeed;
import com.riskstream.runtime.PaymentBus;
import com.riskstream.runtime.RiskstreamProperties;
import com.riskstream.simulator.PaymentSimulator.Scenario;
import com.riskstream.simulator.TrafficGenerator;
import com.riskstream.topology.StreamMetrics;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api/v1")
@Tag(name = "Riskstream", description = "Feed payments in, read alerts out, and see what the stream is doing")
public class RiskController {

    public record ScenarioScore(Scenario scenario, String description, String expectedRule, long injected, long detected) {
    }

    public record Stats(Instant asOf, String runtime, long uptimeSeconds, long published, long processed,
                        double processedPerSecond, long duplicates, long late, long malformed, long alerts,
                        long suppressed, Map<Alert.Rule, Long> alertsByRule, List<ScenarioScore> scenarios,
                        int liveListeners) {
    }

    public record RuleView(Alert.Rule rule, String description, String threshold) {
    }

    public record Injected(Scenario scenario, int payments, String expectedRule) {
    }

    private final PaymentBus bus;
    private final AlertFeed feed;
    private final StreamMetrics metrics;
    private final TrafficGenerator traffic;
    private final RiskstreamProperties properties;
    private final Clock clock;
    private final RequestBudget budget;
    private final Instant startedAt;
    private long sampledAtMillis;
    private long sampledProcessed;
    private double rate;

    public RiskController(PaymentBus bus, AlertFeed feed, StreamMetrics metrics, TrafficGenerator traffic,
                          RiskstreamProperties properties, Clock clock) {
        this.bus = bus;
        this.feed = feed;
        this.metrics = metrics;
        this.traffic = traffic;
        this.properties = properties;
        this.clock = clock;
        this.budget = new RequestBudget(30, Duration.ofSeconds(10), clock);
        this.startedAt = clock.instant();
        this.sampledAtMillis = clock.millis();
    }

    @GetMapping("/stats")
    @Operation(summary = "Throughput, counters, alerts by rule, and how many injected incidents were detected")
    public synchronized Stats stats() {
        Instant now = clock.instant();
        long processed = metrics.processedCount();
        long elapsed = now.toEpochMilli() - sampledAtMillis;
        if (elapsed >= 1_000) {
            rate = Math.round(10_000.0 * (processed - sampledProcessed) / elapsed) / 10.0;
            sampledAtMillis = now.toEpochMilli();
            sampledProcessed = processed;
        }
        Map<Alert.Rule, Long> byRule = new LinkedHashMap<>();
        for (Alert.Rule rule : Alert.Rule.values()) {
            byRule.put(rule, metrics.alertCount(rule));
        }
        List<ScenarioScore> scores = new ArrayList<>();
        for (Scenario scenario : Scenario.values()) {
            if (scenario != Scenario.NORMAL) {
                scores.add(new ScenarioScore(scenario, scenario.description(), scenario.expectedRule(),
                        traffic.injectedCount(scenario), traffic.detectedCount(scenario)));
            }
        }
        return new Stats(now, bus.describe(), Duration.between(startedAt, now).toSeconds(), traffic.publishedCount(),
                processed, rate, metrics.duplicateCount(), metrics.lateCount(), metrics.malformedCount(),
                metrics.alertCount(), metrics.suppressedCount(), byRule, scores, feed.listenerCount());
    }

    @GetMapping("/alerts")
    @Operation(summary = "The most recent alerts, newest first")
    public List<Alert> alerts(@RequestParam(defaultValue = "50") int limit,
                              @RequestParam(required = false) Alert.Rule rule) {
        if (limit < 1 || limit > 500) {
            throw ApiException.badRequest("INVALID_LIMIT", "limit must be between 1 and 500");
        }
        return feed.recent(limit, rule);
    }

    @GetMapping(path = "/alerts/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(summary = "Alerts as they happen, as server-sent events")
    public SseEmitter stream() {
        SseEmitter emitter = feed.listen();
        if (emitter == null) {
            throw new ApiException(HttpStatus.TOO_MANY_REQUESTS, "TOO_MANY_LISTENERS", "Too many live connections; try again later");
        }
        return emitter;
    }

    @GetMapping("/rules")
    @Operation(summary = "The rules and the thresholds they are running with")
    public List<RuleView> rules() {
        RiskstreamProperties.Rules r = properties.rules();
        return List.of(
                new RuleView(Alert.Rule.VELOCITY, Alert.Rule.VELOCITY.description(),
                        "More than " + r.velocityMaxPayments() + " payments within " + r.velocityWindow().toSeconds() + " s"),
                new RuleView(Alert.Rule.SPEND_LIMIT, Alert.Rule.SPEND_LIMIT.description(),
                        "More than " + money(r.spendLimitMinor()) + " within " + r.spendWindow().toMinutes() + " min"),
                new RuleView(Alert.Rule.GEO_JUMP, Alert.Rule.GEO_JUMP.description(),
                        "Two countries within " + r.geoWindow().toMinutes() + " min"),
                new RuleView(Alert.Rule.AMOUNT_SPIKE, Alert.Rule.AMOUNT_SPIKE.description(),
                        "At least " + r.spikeMultiplier() + " times the card average, after " + r.spikeMinHistory()
                                + " payments, and at least " + money(r.spikeMinAmountMinor())));
    }

    @PostMapping("/simulate/{scenario}")
    @Operation(summary = "Play one fraud incident into the stream",
            description = "CARD_TESTING, ACCOUNT_TAKEOVER, BUST_OUT or AMOUNT_SPIKE. The matching alert should appear within a second.")
    public Injected simulate(@PathVariable Scenario scenario) {
        if (scenario == Scenario.NORMAL) {
            throw ApiException.badRequest("NOT_AN_INCIDENT", "NORMAL is ordinary traffic, not an incident");
        }
        budget.spend();
        return new Injected(scenario, traffic.inject(scenario), scenario.expectedRule());
    }

    @PostMapping("/payments")
    @ResponseStatus(HttpStatus.ACCEPTED)
    @Operation(summary = "Publish one payment to the stream",
            description = "Accepted means published, not judged: alerts arrive on the alert stream. "
                    + "cardId is an opaque token. Never send a card number.")
    public Map<String, String> publish(@Valid @RequestBody Payment payment) {
        budget.spend();
        if (payment.occurredAt().isAfter(clock.instant().plus(Duration.ofMinutes(5)))) {
            throw ApiException.unprocessable("FUTURE_PAYMENT", "occurredAt is in the future");
        }
        traffic.send(payment);
        return Map.of("status", "published", "paymentId", payment.paymentId());
    }

    private static String money(long minor) {
        return String.format("%,d.%02d", minor / 100, minor % 100);
    }
}
