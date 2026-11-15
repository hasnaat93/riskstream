package com.riskstream.runtime;

import com.riskstream.model.Alert;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Where alerts land after leaving the stream: the most recent ones kept in memory for the
 * console, and pushed to every browser that is listening. This is a live tail, not a store;
 * a system of record would consume the alerts topic into a database.
 */
@Component
public class AlertFeed {

    static final int CAPACITY = 500;
    private static final int MAX_LISTENERS = 200;

    private final Deque<Alert> recent = new ArrayDeque<>(CAPACITY);
    private final List<SseEmitter> listeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<Alert>> observers = new CopyOnWriteArrayList<>();

    /** Called by the runtime for every alert the topology emits. */
    public void publish(Alert alert) {
        synchronized (recent) {
            if (recent.size() == CAPACITY) {
                recent.removeLast();
            }
            recent.addFirst(alert);
        }
        observers.forEach(observer -> observer.accept(alert));
        for (SseEmitter listener : listeners) {
            try {
                listener.send(SseEmitter.event().name("alert").data(alert));
            } catch (IOException | IllegalStateException gone) {
                listeners.remove(listener);
            }
        }
    }

    /** Newest first. */
    public List<Alert> recent(int limit, Alert.Rule rule) {
        List<Alert> selected = new ArrayList<>();
        synchronized (recent) {
            for (Alert alert : recent) {
                if (rule == null || alert.rule() == rule) {
                    selected.add(alert);
                    if (selected.size() == limit) {
                        break;
                    }
                }
            }
        }
        return selected;
    }

    public void observe(Consumer<Alert> observer) {
        observers.add(observer);
    }

    /** Opens a server-sent event stream of alerts. Returns null when too many are already open. */
    public SseEmitter listen() {
        if (listeners.size() >= MAX_LISTENERS) {
            return null;
        }
        SseEmitter emitter = new SseEmitter(0L);
        emitter.onCompletion(() -> listeners.remove(emitter));
        emitter.onTimeout(() -> listeners.remove(emitter));
        emitter.onError(error -> listeners.remove(emitter));
        listeners.add(emitter);
        return emitter;
    }

    public int listenerCount() {
        return listeners.size();
    }
}
