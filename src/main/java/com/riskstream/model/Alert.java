package com.riskstream.model;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * A rule firing on a payment.
 *
 * @param alertId   derived from the payment and the rule, so reprocessing the same payment
 *                  produces the same alert id and a consumer can discard the repeat
 * @param eventTime the event time of the payment that triggered the rule
 * @param evidence  the numbers the rule based its decision on
 */
public record Alert(String alertId, Rule rule, Severity severity, String cardId, String paymentId,
                    Instant eventTime, String summary, Map<String, Object> evidence) {

    public enum Rule {
        VELOCITY("Too many payments on one card in a short window"),
        SPEND_LIMIT("Total spend on one card over a limit within a window"),
        GEO_JUMP("The same card used in two countries too close together in time"),
        AMOUNT_SPIKE("A payment far above the card's usual amount");

        private final String description;

        Rule(String description) {
            this.description = description;
        }

        public String description() {
            return description;
        }
    }

    public enum Severity { MEDIUM, HIGH }

    public static Alert of(Rule rule, Severity severity, Payment payment, String summary, Map<String, Object> evidence) {
        String id = UUID.nameUUIDFromBytes((payment.paymentId() + "|" + rule.name()).getBytes(StandardCharsets.UTF_8))
                .toString();
        return new Alert(id, rule, severity, payment.cardId(), payment.paymentId(), payment.occurredAt(), summary, evidence);
    }
}
