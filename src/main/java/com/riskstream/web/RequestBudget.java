package com.riskstream.web;

import com.riskstream.common.ApiException;
import java.time.Clock;
import java.time.Duration;
import org.springframework.http.HttpStatus;

/**
 * A small shared allowance for the endpoints that write to the stream. The service has no
 * sign-in, so on a public demo this is what stops one visitor flooding it. It is one budget
 * for everybody, not a per-client limit.
 */
final class RequestBudget {

    private final int allowance;
    private final long windowMillis;
    private final Clock clock;
    private long windowStart;
    private int spent;

    RequestBudget(int allowance, Duration window, Clock clock) {
        this.allowance = allowance;
        this.windowMillis = window.toMillis();
        this.clock = clock;
        this.windowStart = clock.millis();
    }

    synchronized void spend() {
        long now = clock.millis();
        if (now - windowStart >= windowMillis) {
            windowStart = now;
            spent = 0;
        }
        if (spent >= allowance) {
            throw new ApiException(HttpStatus.TOO_MANY_REQUESTS, "RATE_LIMITED",
                    "Too many requests are writing to the stream right now. Try again in a few seconds.");
        }
        spent++;
    }
}
