package com.example.loadbalancer;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * A time-based sliding ("floating") window made of a fixed number of time buckets.
 *
 * <p>The window does not reset on a timer. Instead, every observation is written into the
 * bucket that covers its instant, and {@link #snapshot()} only sums the buckets that overlap
 * the last {@code window} duration. Old observations therefore leave the window one bucket
 * at a time, which keeps recently recorded behavior available without a scheduled eviction
 * thread.
 *
 * <p>The window retains at most {@code window} worth of bucket-aligned data. In the worst
 * case the effective sample interval is one bucket shorter than the configured window, which
 * is the normal trade-off for a fixed-bucket implementation.
 *
 * <p>This class is thread-safe. Recording and snapshotting are synchronized; bucket arrays
 * are short (typically 10-20 entries), so contention is minimal.
 */
public final class FloatingWindow {

    private final long windowNanos;
    private final long bucketNanos;
    private final int bucketCount;
    private final Bucket[] buckets;
    private final LongSupplier nanoClock;
    private final long originNanos;

    /**
     * Creates a window backed by {@link System#nanoTime()}.
     *
     * @param window      total time span to retain; must be positive
     * @param bucketCount number of time buckets; must be at least one and no more than
     *                    {@code window.toNanos()}
     */
    public FloatingWindow(Duration window, int bucketCount) {
        this(window, bucketCount, System::nanoTime);
    }

    /**
     * Creates a window with a custom nanosecond clock. Visible for tests.
     */
    FloatingWindow(Duration window, int bucketCount, LongSupplier nanoClock) {
        Objects.requireNonNull(window, "window");
        Objects.requireNonNull(nanoClock, "nanoClock");
        if (window.isZero() || window.isNegative()) {
            throw new IllegalArgumentException("window must be positive");
        }
        if (bucketCount < 1) {
            throw new IllegalArgumentException("bucketCount must be >= 1");
        }

        final long windowNanos;
        try {
            windowNanos = window.toNanos();
        } catch (ArithmeticException ex) {
            throw new IllegalArgumentException("window is too large", ex);
        }
        if (windowNanos < bucketCount) {
            throw new IllegalArgumentException(
                    "window must be at least " + bucketCount + " nanoseconds for " + bucketCount + " buckets");
        }

        this.windowNanos = windowNanos;
        this.bucketCount = bucketCount;
        this.bucketNanos = windowNanos / bucketCount;
        this.buckets = new Bucket[bucketCount];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new Bucket();
        }
        this.nanoClock = nanoClock;
        this.originNanos = nanoClock.getAsLong();
    }

    /**
     * Records one completed request. A negative latency is rejected so window averages cannot
     * be corrupted by bad instrumentation.
     *
     * @param success      whether the request completed successfully
     * @param latencyNanos elapsed time of the request in nanoseconds
     */
    public void record(boolean success, long latencyNanos) {
        if (latencyNanos < 0) {
            throw new IllegalArgumentException("latencyNanos must be >= 0");
        }

        synchronized (this) {
            long epoch = epochOf(nanoClock.getAsLong());
            int slot = Math.floorMod(epoch, bucketCount);
            Bucket bucket = buckets[slot];
            if (bucket.epoch != epoch) {
                bucket.epoch = epoch;
                bucket.requests = 0;
                bucket.failures = 0;
                bucket.latencyNanos = 0;
            }
            bucket.requests++;
            if (!success) {
                bucket.failures++;
            }
            bucket.latencyNanos += latencyNanos;
        }
    }

    /**
     * Returns the aggregate of every bucket that currently overlaps the window. Buckets from
     * the future (possible only if the clock moves backwards) are ignored.
     */
    public synchronized WindowSnapshot snapshot() {
        long currentEpoch = epochOf(nanoClock.getAsLong());
        long oldestEpoch = currentEpoch - bucketCount + 1;

        long requests = 0;
        long failures = 0;
        long latencyNanos = 0;
        for (Bucket bucket : buckets) {
            if (bucket.epoch >= oldestEpoch && bucket.epoch <= currentEpoch) {
                requests += bucket.requests;
                failures += bucket.failures;
                latencyNanos += bucket.latencyNanos;
            }
        }
        return new WindowSnapshot(windowNanos, requests, failures, latencyNanos);
    }

    /**
     * Drops every observation. Mostly useful for tests and administrative resets.
     */
    public synchronized void clear() {
        for (Bucket bucket : buckets) {
            bucket.epoch = Long.MIN_VALUE;
            bucket.requests = 0;
            bucket.failures = 0;
            bucket.latencyNanos = 0;
        }
    }

    public Duration window() {
        return Duration.ofNanos(windowNanos);
    }

    public int bucketCount() {
        return bucketCount;
    }

    private long epochOf(long nowNanos) {
        return Math.floorDiv(nowNanos - originNanos, bucketNanos);
    }

    private static final class Bucket {
        private long epoch = Long.MIN_VALUE;
        private long requests;
        private long failures;
        private long latencyNanos;
    }
}
