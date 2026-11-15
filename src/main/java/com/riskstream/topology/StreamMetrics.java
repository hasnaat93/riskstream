package com.riskstream.topology;

import com.riskstream.model.Alert;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Counters the processor updates as it works. Shared by every stream thread, so atomic. */
public final class StreamMetrics {

    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong late = new AtomicLong();
    private final AtomicLong malformed = new AtomicLong();
    private final AtomicLong suppressed = new AtomicLong();
    private final Map<Alert.Rule, AtomicLong> alerts = new EnumMap<>(Alert.Rule.class);

    public StreamMetrics() {
        for (Alert.Rule rule : Alert.Rule.values()) {
            alerts.put(rule, new AtomicLong());
        }
    }

    void processed() {
        processed.incrementAndGet();
    }

    void duplicate() {
        duplicates.incrementAndGet();
    }

    void late() {
        late.incrementAndGet();
    }

    void malformed() {
        malformed.incrementAndGet();
    }

    void suppressed() {
        suppressed.incrementAndGet();
    }

    void alert(Alert.Rule rule) {
        alerts.get(rule).incrementAndGet();
    }

    /** Payments evaluated against the rules. */
    public long processedCount() {
        return processed.get();
    }

    /** Payments discarded because their id had been seen before. */
    public long duplicateCount() {
        return duplicates.get();
    }

    /** Payments discarded for arriving later than the allowed lateness. */
    public long lateCount() {
        return late.get();
    }

    /** Records that carried no usable payment. */
    public long malformedCount() {
        return malformed.get();
    }

    /** Rule firings held back because the same rule had just fired for the same card. */
    public long suppressedCount() {
        return suppressed.get();
    }

    public long alertCount(Alert.Rule rule) {
        return alerts.get(rule).get();
    }

    public long alertCount() {
        return alerts.values().stream().mapToLong(AtomicLong::get).sum();
    }
}
