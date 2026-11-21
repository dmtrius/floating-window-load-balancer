package com.example.loadbalancer;

import java.time.Duration;

/**
 * Immutable aggregate of the observations currently inside a {@link FloatingWindow}.
 *
 * @param windowNanos length of the window that produced this snapshot
 * @param requests    completed requests observed in the window
 * @param failures    failed requests observed in the window
 * @param latencyNanos sum of request latencies observed in the window
 */
public record WindowSnapshot(long windowNanos, long requests, long failures, long latencyNanos) {

    public WindowSnapshot {
        if (windowNanos <= 0) {
            throw new IllegalArgumentException("windowNanos must be > 0");
        }
        if (requests < 0 || failures < 0 || failures > requests || latencyNanos < 0) {
            throw new IllegalArgumentException("invalid window counters");
        }
    }

    /** Failure ratio in {@code [0, 1]}; zero when no requests were observed. */
    public double failureRate() {
        return requests == 0 ? 0.0 : (double) failures / requests;
    }

    /** Mean latency in nanoseconds; zero when no requests were observed. */
    public double averageLatencyNanos() {
        return requests == 0 ? 0.0 : (double) latencyNanos / requests;
    }

    /** Mean latency as a {@link Duration}; zero when no requests were observed. */
    public Duration averageLatency() {
        return Duration.ofNanos((long) averageLatencyNanos());
    }

    /** Throughput represented by this window, in requests per second. */
    public double requestsPerSecond() {
        return requests * 1_000_000_000.0 / windowNanos;
    }

    /** Configured window length. */
    public Duration window() {
        return Duration.ofNanos(windowNanos);
    }
}
