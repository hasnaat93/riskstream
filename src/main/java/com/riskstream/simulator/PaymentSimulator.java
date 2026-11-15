package com.riskstream.simulator;

import com.riskstream.model.Payment;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * A deterministic source of fictional payments: ordinary traffic across a pool of cards, and
 * on request one of four fraud patterns on a card that already has a history.
 *
 * <p>Every payment it produces is labelled with the scenario it belongs to. The labels never
 * travel on the stream; they exist so that tests can measure what the rules caught and what
 * they flagged by mistake. The same seed always produces the same payments.
 */
public final class PaymentSimulator {

    public enum Scenario {
        NORMAL("Ordinary traffic", null),
        CARD_TESTING("A burst of small payments on one card, as when stolen card details are tried out", "VELOCITY"),
        ACCOUNT_TAKEOVER("A payment abroad minutes after one at home", "GEO_JUMP"),
        BUST_OUT("Several large payments in quick succession", "SPEND_LIMIT"),
        AMOUNT_SPIKE("One payment far above what the card usually spends", "AMOUNT_SPIKE");

        private final String description;
        private final String expectedRule;

        Scenario(String description, String expectedRule) {
            this.description = description;
            this.expectedRule = expectedRule;
        }

        public String description() {
            return description;
        }

        /** The rule that should catch this scenario, or null for ordinary traffic. */
        public String expectedRule() {
            return expectedRule;
        }
    }

    /** A payment with what the simulator knows about it. incidentId is null for ordinary traffic. */
    public record Labelled(Payment payment, Scenario scenario, String incidentId) {
    }

    private static final String[] HOME = {"US", "US", "US", "GB", "GB", "DE", "FR", "PK", "PK", "AE", "CA", "NL"};
    private static final String[] FAR = {"BR", "NG", "RU", "VN", "ID", "MX"};
    private static final String[] CHANNELS = {"ECOMMERCE", "ECOMMERCE", "POS", "POS", "MOBILE"};

    private final Random random;
    private final String idPrefix;
    private final int cards;
    private final String[] cardCountry;
    private final long[] cardTypical;
    private final int[] cardPayments;
    private long paymentSequence;
    private long incidentSequence;

    public PaymentSimulator(long seed, int cards) {
        this(seed, cards, "pay-");
    }

    /**
     * @param idPrefix start of every payment id. A long-lived deployment gives each run its own
     *                 prefix: ids restart from 1 with the process, and the stream rightly discards
     *                 a payment id it has already seen.
     */
    public PaymentSimulator(long seed, int cards, String idPrefix) {
        this.idPrefix = idPrefix;
        if (cards < 10) {
            throw new IllegalArgumentException("The simulator needs at least 10 cards");
        }
        this.random = new Random(seed);
        this.cards = cards;
        this.cardCountry = new String[cards];
        this.cardTypical = new long[cards];
        this.cardPayments = new int[cards];
        for (int i = 0; i < cards; i++) {
            cardCountry[i] = HOME[random.nextInt(HOME.length)];
            cardTypical[i] = 1_500 + (long) (6_500 * random.nextDouble() * random.nextDouble());   // 15.00 to 80.00
        }
    }

    public static String cardId(int index) {
        return String.format("card-%06d", index);
    }

    /** One ordinary payment on a random card at the given time. */
    public Labelled normal(Instant at) {
        int card = random.nextInt(cards);
        return new Labelled(payment(card, honestAmount(card), cardCountry[card], at, merchant()), Scenario.NORMAL, null);
    }

    /** {@code count} ordinary payments spread evenly, with jitter, over a period. Oldest first. */
    public List<Labelled> normal(Instant from, Duration span, int count) {
        List<Labelled> payments = new ArrayList<>(count);
        long step = Math.max(1, span.toMillis() / Math.max(1, count));
        for (int i = 0; i < count; i++) {
            payments.add(normal(from.plusMillis(i * step + random.nextLong(step))));
        }
        return payments;
    }

    /**
     * The payments of one fraud incident starting at the given time, oldest first. The victim
     * card first gets enough ordinary history, dated before the incident, for the rules that
     * compare with a card's past to have something to compare with.
     */
    public List<Labelled> incident(Scenario scenario, Instant at) {
        if (scenario == Scenario.NORMAL) {
            throw new IllegalArgumentException("NORMAL is not an incident");
        }
        int card = random.nextInt(cards);
        String incidentId = String.format("incident-%05d", ++incidentSequence);
        List<Labelled> payments = new ArrayList<>();

        // History: ordinary use in the minutes before, spaced far enough apart to trip no rule
        // itself and recent enough to be inside the lateness the stream accepts.
        int missing = Math.max(0, 8 - cardPayments[card]);
        for (int i = missing; i >= 1; i--) {
            Instant when = at.minusSeconds(90L * i + random.nextInt(20));
            payments.add(new Labelled(payment(card, ordinaryAmount(card), cardCountry[card], when, merchant()),
                    Scenario.NORMAL, null));
        }

        switch (scenario) {
            case CARD_TESTING -> {
                int attempts = 8 + random.nextInt(7);
                String merchant = "merchant-test-" + random.nextInt(5);
                for (int i = 0; i < attempts; i++) {
                    Instant when = at.plusMillis(i * (2_000L + random.nextInt(1_500)));
                    payments.add(labelled(card, 100 + random.nextInt(400), cardCountry[card], when, merchant, scenario, incidentId));
                }
            }
            case ACCOUNT_TAKEOVER -> {
                payments.add(labelled(card, ordinaryAmount(card), cardCountry[card], at, merchant(), scenario, incidentId));
                Instant abroad = at.plus(Duration.ofMinutes(2 + random.nextInt(8))).plusSeconds(random.nextInt(60));
                payments.add(labelled(card, ordinaryAmount(card) * 3, FAR[random.nextInt(FAR.length)], abroad,
                        merchant(), scenario, incidentId));
            }
            case BUST_OUT -> {
                int purchases = 4 + random.nextInt(3);
                for (int i = 0; i < purchases; i++) {
                    Instant when = at.plusSeconds(i * (70L + random.nextInt(40)));
                    payments.add(labelled(card, 70_000 + random.nextInt(25_000), cardCountry[card], when, merchant(),
                            scenario, incidentId));
                }
            }
            case AMOUNT_SPIKE -> payments.add(labelled(card, cardTypical[card] * (14 + random.nextInt(12)),
                    cardCountry[card], at, merchant(), scenario, incidentId));
            default -> throw new IllegalStateException("Unhandled scenario " + scenario);
        }
        return payments;
    }

    /** The longest an incident of this kind lasts, from its first payment to its last. */
    public static Duration longestSpan(Scenario scenario) {
        return switch (scenario) {
            case CARD_TESTING -> Duration.ofSeconds(50);
            case ACCOUNT_TAKEOVER -> Duration.ofMinutes(11);
            case BUST_OUT -> Duration.ofMinutes(10);
            default -> Duration.ZERO;
        };
    }

    private Labelled labelled(int card, long amount, String country, Instant at, String merchant, Scenario scenario,
                              String incidentId) {
        return new Labelled(payment(card, amount, country, at, merchant), scenario, incidentId);
    }

    private Payment payment(int card, long amountMinor, String country, Instant at, String merchant) {
        cardPayments[card]++;
        return new Payment(idPrefix + String.format("%010d", ++paymentSequence), cardId(card), merchant, amountMinor, "USD",
                country, CHANNELS[random.nextInt(CHANNELS.length)], at);
    }

    /** Around the card's typical amount, and never more than three times it. */
    private long ordinaryAmount(int card) {
        double factor = Math.min(3.0, Math.exp(random.nextGaussian() * 0.45));
        return Math.max(100, Math.round(cardTypical[card] * factor));
    }

    /**
     * What an honest customer spends. Usually an ordinary amount, but about one payment in a
     * hundred is a genuine large purchase, five to twelve times the usual. Those are not
     * fraud, and a rule that flags them is making a mistake the tests can count.
     */
    private long honestAmount(int card) {
        if (random.nextInt(100) == 0) {
            return cardTypical[card] * (5 + random.nextInt(8));
        }
        return ordinaryAmount(card);
    }

    private String merchant() {
        return "merchant-" + (1000 + random.nextInt(400));
    }
}
