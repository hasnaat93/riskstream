package com.riskstream.topology;

import com.riskstream.model.Alert;
import com.riskstream.model.Alert.Rule;
import com.riskstream.model.Alert.Severity;
import com.riskstream.model.Payment;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Evaluates the risk rules for one payment against what is known about its card.
 *
 * <p>Everything is in <b>event time</b>: the time on the payment, not the time it arrived.
 * Events can arrive out of order, so a window is never "the last minute before now". For a
 * payment at time T, the velocity and spend rules look at every window of the rule's length
 * that contains T, on both sides of it. A burst therefore raises an alert whichever of its
 * payments happens to arrive last.
 *
 * <p>All state is one record per card in a key-value store: its recent payments, the payment
 * ids already seen, its lifetime average, and when each rule last fired. Handling a payment
 * is therefore one read and one write, whatever the total traffic. (An earlier version used
 * Kafka Streams window stores; in memory those scan every timestamp in the requested range,
 * so the cost of each lookup grew with the traffic of all cards. See docs/design.md.)
 */
public final class RiskProcessor implements Processor<String, Payment, String, Alert> {

    static final String CARD_STATE = "card-state";

    /**
     * Everything remembered about one card.
     *
     * @param payments   lifetime number of payments, for the average
     * @param totalMinor lifetime total, for the average
     * @param recent     payments within the retention period, oldest first
     * @param seen       payment id to event time, within the dedupe retention
     * @param lastFired  rule name to the event time it last fired for this card
     */
    public record CardState(long payments, long totalMinor, List<Payment> recent, Map<String, Long> seen,
                            Map<String, Long> lastFired) {

        static CardState empty() {
            return new CardState(0, 0, new ArrayList<>(), new HashMap<>(), new HashMap<>());
        }

        long averageMinor() {
            return payments == 0 ? 0 : totalMinor / payments;
        }
    }

    private final RuleSettings settings;
    private final StreamMetrics metrics;
    private ProcessorContext<String, Alert> context;
    private KeyValueStore<String, CardState> cards;

    public RiskProcessor(RuleSettings settings, StreamMetrics metrics) {
        this.settings = settings;
        this.metrics = metrics;
    }

    @Override
    public void init(ProcessorContext<String, Alert> context) {
        this.context = context;
        this.cards = context.getStateStore(CARD_STATE);
    }

    @Override
    public void process(Record<String, Payment> record) {
        Payment received = record.value();
        if (received == null || received.cardId() == null || received.paymentId() == null || received.occurredAt() == null) {
            metrics.malformed();
            return;
        }
        // The record timestamp is the event time the stream settled on (see RiskTopology.eventTime).
        // It normally equals the time on the payment; where a payment claimed to be from the
        // future it has been capped, and from here on the capped time is the payment's time.
        long time = record.timestamp();
        Payment payment = received.occurredAt().toEpochMilli() == time ? received
                : new Payment(received.paymentId(), received.cardId(), received.merchantId(), received.amountMinor(),
                        received.currency(), received.country(), received.channel(), Instant.ofEpochMilli(time));

        // Too old to judge: the history it would need has already been let go.
        long streamTime = context.currentStreamTimeMs();
        if (time < streamTime - settings.allowedLateness().toMillis()) {
            metrics.late();
            return;
        }

        CardState stored = cards.get(payment.cardId());
        CardState state = stored == null ? CardState.empty() : mutableCopy(stored);

        // Delivery is at-least-once, so the same payment can arrive twice. Only the first counts.
        if (state.seen().containsKey(payment.paymentId())) {
            metrics.duplicate();
            return;
        }
        metrics.processed();
        state.seen().put(payment.paymentId(), time);
        state.recent().add(payment);
        state.recent().sort(Comparator.comparing(Payment::occurredAt).thenComparing(Payment::paymentId));

        List<Alert> alerts = new ArrayList<>(4);
        velocity(payment, state.recent(), alerts);
        spend(payment, state.recent(), alerts);
        geoJump(payment, state.recent(), alerts);
        amountSpike(payment, state, alerts);

        for (Alert alert : alerts) {
            Long lastFired = state.lastFired().get(alert.rule().name());
            if (lastFired != null && Math.abs(time - lastFired) < settings.cooldown().toMillis()) {
                metrics.suppressed();
                continue;
            }
            state.lastFired().put(alert.rule().name(), time);
            metrics.alert(alert.rule());
            context.forward(record.withKey(alert.cardId()).withValue(alert));
        }

        forget(state, Math.max(streamTime, time));
        cards.put(payment.cardId(), new CardState(state.payments() + 1, state.totalMinor() + payment.amountMinor(),
                state.recent(), state.seen(), state.lastFired()));
    }

    // ---------------------------------------------------------------- state

    private static CardState mutableCopy(CardState state) {
        return new CardState(state.payments(), state.totalMinor(), new ArrayList<>(state.recent()),
                new HashMap<>(state.seen()), new HashMap<>(state.lastFired()));
    }

    /**
     * Lets go of what can no longer matter, so a card's record stays small however long the
     * card lives. A payment stays while a late arrival could still need it as a neighbour.
     */
    private void forget(CardState state, long now) {
        long reach = settings.allowedLateness().toMillis() + settings.longestWindow().toMillis();
        state.recent().removeIf(p -> p.occurredAt().toEpochMilli() < now - reach);
        long seenCutoff = now - Math.max(settings.dedupeRetention().toMillis(), reach);
        state.seen().values().removeIf(seenAt -> seenAt < seenCutoff);
        long firedCutoff = now - settings.cooldown().toMillis() - settings.allowedLateness().toMillis();
        state.lastFired().values().removeIf(firedAt -> firedAt < firedCutoff);
    }

    // ---------------------------------------------------------------- rules

    /**
     * The busiest window of the given length that contains the payment at {@code index}:
     * busiest by number of payments, or by total amount. Every window that starts at one of
     * the card's payments and still reaches the judged payment is tried. The list is short
     * (one card, within the retention period), so the simple loop is the right one.
     *
     * @return {first index, last index} of that window within {@code payments}
     */
    static int[] busiestWindowContaining(List<Payment> payments, int index, Duration window, boolean byAmount) {
        long length = window.toMillis();
        long judged = payments.get(index).occurredAt().toEpochMilli();
        int[] best = {index, index};
        long bestScore = -1;
        for (int first = 0; first <= index; first++) {
            long start = payments.get(first).occurredAt().toEpochMilli();
            if (judged >= start + length) {
                continue;       // a window starting here ends before the judged payment
            }
            int last = first;
            long total = payments.get(first).amountMinor();
            while (last + 1 < payments.size() && payments.get(last + 1).occurredAt().toEpochMilli() < start + length) {
                last++;
                total += payments.get(last).amountMinor();
            }
            long score = byAmount ? total : last - first + 1;
            if (score > bestScore) {
                bestScore = score;
                best = new int[] {first, last};
            }
        }
        return best;
    }

    private static int indexOf(List<Payment> payments, Payment payment) {
        for (int i = 0; i < payments.size(); i++) {
            if (payments.get(i).paymentId().equals(payment.paymentId())) {
                return i;
            }
        }
        throw new IllegalStateException("The payment being judged is missing from its own history");
    }

    private void velocity(Payment payment, List<Payment> recent, List<Alert> alerts) {
        int[] window = busiestWindowContaining(recent, indexOf(recent, payment), settings.velocityWindow(), false);
        int count = window[1] - window[0] + 1;
        if (count > settings.velocityMaxPayments()) {
            Map<String, Object> evidence = new LinkedHashMap<>();
            evidence.put("payments", count);
            evidence.put("windowSeconds", settings.velocityWindow().toSeconds());
            evidence.put("limit", settings.velocityMaxPayments());
            evidence.put("firstAt", recent.get(window[0]).occurredAt().toString());
            evidence.put("lastAt", recent.get(window[1]).occurredAt().toString());
            alerts.add(Alert.of(Rule.VELOCITY, Severity.HIGH, payment,
                    count + " payments on one card within " + settings.velocityWindow().toSeconds() + " s", evidence));
        }
    }

    private void spend(Payment payment, List<Payment> recent, List<Alert> alerts) {
        int[] window = busiestWindowContaining(recent, indexOf(recent, payment), settings.spendWindow(), true);
        long total = 0;
        for (int i = window[0]; i <= window[1]; i++) {
            total += recent.get(i).amountMinor();
        }
        if (total > settings.spendLimitMinor()) {
            Map<String, Object> evidence = new LinkedHashMap<>();
            evidence.put("totalMinor", total);
            evidence.put("limitMinor", settings.spendLimitMinor());
            evidence.put("payments", window[1] - window[0] + 1);
            evidence.put("windowMinutes", settings.spendWindow().toMinutes());
            alerts.add(Alert.of(Rule.SPEND_LIMIT, Severity.MEDIUM, payment,
                    "Spend of " + money(total) + " within " + settings.spendWindow().toMinutes() + " min, over the limit of "
                            + money(settings.spendLimitMinor()), evidence));
        }
    }

    private void geoJump(Payment payment, List<Payment> recent, List<Alert> alerts) {
        long time = payment.occurredAt().toEpochMilli();
        Payment closest = null;
        for (Payment other : recent) {
            long gap = Math.abs(other.occurredAt().toEpochMilli() - time);
            if (!other.country().equals(payment.country()) && gap <= settings.geoWindow().toMillis()
                    && (closest == null || gap < Math.abs(closest.occurredAt().toEpochMilli() - time))) {
                closest = other;
            }
        }
        if (closest != null) {
            long minutes = Math.abs(closest.occurredAt().toEpochMilli() - time) / 60_000;
            Map<String, Object> evidence = new LinkedHashMap<>();
            evidence.put("country", payment.country());
            evidence.put("otherCountry", closest.country());
            evidence.put("otherPaymentId", closest.paymentId());
            evidence.put("minutesApart", minutes);
            alerts.add(Alert.of(Rule.GEO_JUMP, Severity.HIGH, payment,
                    "Used in " + closest.country() + " and " + payment.country() + " " + minutes + " min apart", evidence));
        }
    }

    /** Compares the payment with the card's lifetime average, as it stood before this payment. */
    private void amountSpike(Payment payment, CardState state, List<Alert> alerts) {
        if (state.payments() >= settings.spikeMinHistory()
                && payment.amountMinor() >= settings.spikeMinAmountMinor()
                && payment.amountMinor() >= settings.spikeMultiplier() * state.averageMinor()) {
            Map<String, Object> evidence = new LinkedHashMap<>();
            evidence.put("amountMinor", payment.amountMinor());
            evidence.put("cardAverageMinor", state.averageMinor());
            evidence.put("timesAverage", Math.round(10.0 * payment.amountMinor() / Math.max(1, state.averageMinor())) / 10.0);
            evidence.put("earlierPayments", state.payments());
            alerts.add(Alert.of(Rule.AMOUNT_SPIKE, Severity.MEDIUM, payment,
                    money(payment.amountMinor()) + " against a card average of " + money(state.averageMinor()), evidence));
        }
    }

    private static String money(long minor) {
        return String.format("%d.%02d", minor / 100, minor % 100);
    }
}
