package com.riskstream.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.time.Instant;

/**
 * One payment attempt, as it arrives on the stream.
 *
 * <p>{@code cardId} is an opaque token, never a card number: this system has no use for one
 * and must not be given one.
 *
 * @param occurredAt when the payment happened (event time). Rules are evaluated against this,
 *                   not against the time the event happened to reach the processor.
 */
public record Payment(
        @NotBlank @Size(max = 64) String paymentId,
        @NotBlank @Size(max = 64) String cardId,
        @NotBlank @Size(max = 64) String merchantId,
        @Positive long amountMinor,
        @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency,
        @NotBlank @Pattern(regexp = "[A-Z]{2}") String country,
        @NotBlank @Size(max = 20) String channel,
        @NotNull Instant occurredAt) {
}
