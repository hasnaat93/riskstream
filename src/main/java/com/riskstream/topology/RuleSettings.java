package com.riskstream.topology;

import java.time.Duration;

/**
 * Thresholds of the four rules, plus how long state is kept. All durations are event time.
 *
 * @param velocityMaxPayments  alert when a card makes more than this many payments ...
 * @param velocityWindow       ... within this window
 * @param spendLimitMinor      alert when a card's total spend exceeds this (minor units) ...
 * @param spendWindow          ... within this window
 * @param geoWindow            alert when a card is used in two countries within this time
 * @param spikeMultiplier      alert when a payment is at least this many times the card's average ...
 * @param spikeMinHistory      ... once the card has at least this many earlier payments ...
 * @param spikeMinAmountMinor  ... and the payment is at least this large
 * @param cooldown             after a rule fires for a card, it stays quiet for that card this long
 * @param dedupeRetention      how long a payment id is remembered, to discard repeats
 * @param allowedLateness      a payment older than this, relative to the newest event seen, is dropped
 */
public record RuleSettings(
        int velocityMaxPayments, Duration velocityWindow,
        long spendLimitMinor, Duration spendWindow,
        Duration geoWindow,
        double spikeMultiplier, int spikeMinHistory, long spikeMinAmountMinor,
        Duration cooldown, Duration dedupeRetention, Duration allowedLateness) {

    public RuleSettings {
        if (velocityMaxPayments < 1 || spendLimitMinor < 1 || spikeMultiplier <= 1 || spikeMinHistory < 1) {
            throw new IllegalArgumentException("Rule thresholds must be positive, and the spike multiplier above 1");
        }
        for (Duration d : new Duration[] {velocityWindow, spendWindow, geoWindow, cooldown, dedupeRetention, allowedLateness}) {
            if (d == null || d.isNegative() || d.isZero()) {
                throw new IllegalArgumentException("Rule durations must be positive");
            }
        }
        if (allowedLateness.compareTo(historyRetention(velocityWindow, spendWindow, geoWindow)) < 0) {
            throw new IllegalArgumentException("allowedLateness must be at least as long as the longest rule window");
        }
    }

    public static RuleSettings defaults() {
        return new RuleSettings(5, Duration.ofSeconds(60), 250_000, Duration.ofMinutes(10), Duration.ofMinutes(30),
                8.0, 5, 10_000, Duration.ofMinutes(5), Duration.ofHours(1), Duration.ofMinutes(30));
    }

    /** How far back the rules ever look: the longest of their windows. */
    public Duration longestWindow() {
        return historyRetention(velocityWindow, spendWindow, geoWindow);
    }

    private static Duration historyRetention(Duration... windows) {
        Duration longest = Duration.ZERO;
        for (Duration w : windows) {
            longest = w.compareTo(longest) > 0 ? w : longest;
        }
        return longest;
    }
}
