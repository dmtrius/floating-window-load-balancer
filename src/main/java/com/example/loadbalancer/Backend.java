package com.example.loadbalancer;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * One backend instance (for example, an HTTP service node).
 *
 * <p>A backend owns its floating window of recent request outcomes and its current in-flight
 * request count. The window is the source of the slow/failing signals used by
 * {@link FloatingWindowStrategy}; the in-flight count provides an immediate signal for
 * concurrency-aware balancing.
 *
 * <p>Availability is a local administrative flag. It is intentionally simple: this project
 * is transport-agnostic and leaves active health checking to the caller.
 */
public final class Backend {

    private final String id;
    private final int weight;
    private final FloatingWindow window;
    private final AtomicInteger inFlight = new AtomicInteger();
    private volatile boolean available = true;

    /**
     * @param id          unique backend identifier
     * @param weight      relative capacity; must be positive
     * @param window      time span used for recent latency and error statistics
     * @param bucketCount number of buckets in the floating window
     */
    public Backend(String id, int weight, Duration window, int bucketCount) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (weight < 1) {
            throw new IllegalArgumentException("weight must be >= 1");
        }
        this.id = id;
        this.weight = weight;
        this.window = new FloatingWindow(window, bucketCount);
    }

    /** Creates a backend with the default weight of one. */
    public Backend(String id, Duration window, int bucketCount) {
        this(id, 1, window, bucketCount);
    }

    public String id() {
        return id;
    }

    /** Relative capacity. A weight of two is treated as twice the capacity of weight one. */
    public int weight() {
        return weight;
    }

    /** Requests currently being processed by this backend. */
    public int inFlightRequests() {
        return inFlight.get();
    }

    public boolean isAvailable() {
        return available;
    }

    /**
     * Marks the backend as available or drained. An unavailable backend is excluded from
     * selection until it is marked available again.
     */
    public void setAvailable(boolean available) {
        this.available = available;
    }

    /** Returns the current floating-window statistics. */
    public WindowSnapshot windowSnapshot() {
        return window.snapshot();
    }

    /**
     * Records a successful request. Useful when callers manage execution themselves instead
     * of using {@link LoadBalancer#execute}.
     */
    public void recordSuccess(Duration latency) {
        window.record(true, toNanos(latency));
    }

    /**
     * Records a failed request. Useful when callers manage execution themselves instead of
     * using {@link LoadBalancer#execute}.
     */
    public void recordFailure(Duration latency) {
        window.record(false, toNanos(latency));
    }

    void beginRequest() {
        inFlight.incrementAndGet();
    }

    void endRequest() {
        inFlight.decrementAndGet();
    }

    void recordOutcome(boolean success, long latencyNanos) {
        window.record(success, latencyNanos);
    }

    private static long toNanos(Duration latency) {
        Objects.requireNonNull(latency, "latency");
        if (latency.isNegative()) {
            throw new IllegalArgumentException("latency must be >= 0");
        }
        return latency.toNanos();
    }

    @Override
    public String toString() {
        return "Backend{" + id + ", weight=" + weight + ", inFlight=" + inFlight.get() + '}';
    }
}
