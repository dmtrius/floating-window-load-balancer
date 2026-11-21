package com.example.loadbalancer;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scores every available backend from its floating window and picks the lowest score.
 *
 * <p>The score combines three signals:
 *
 * <pre>
 * score = ((inFlight + 1) / weight)
 *       * (1 + averageLatency / latencyBaseline)
 *       * (1 + failureRate * errorPenalty)
 * </pre>
 *
 * <p>The {@code +1} makes an idle backend's expected cost non-zero, so selection still spreads
 * load instead of always choosing the first idle node. Latency and failure rate come from the
 * floating window, which means a backend that was slow or failing begins receiving traffic
 * again automatically as those observations age out. The weight divides the concurrency term,
 * so a backend with weight two may carry twice the in-flight work for the same score.
 *
 * <p>Ties are resolved round-robin so equal backends receive fair shares.
 */
public final class FloatingWindowStrategy implements SelectionStrategy {

    /** Latency at which the latency multiplier doubles, by default 50 ms. */
    public static final Duration DEFAULT_LATENCY_BASELINE = Duration.ofMillis(50);

    /** Default multiplier applied to a backend's failure rate. */
    public static final double DEFAULT_ERROR_PENALTY = 10.0;

    private final double latencyBaselineNanos;
    private final double errorPenalty;
    private final AtomicInteger cursor = new AtomicInteger();

    public FloatingWindowStrategy() {
        this(DEFAULT_LATENCY_BASELINE, DEFAULT_ERROR_PENALTY);
    }

    /**
     * @param latencyBaseline latency that doubles a backend's latency penalty; positive
     * @param errorPenalty    added multiplier per unit of failure rate; zero or greater
     */
    public FloatingWindowStrategy(Duration latencyBaseline, double errorPenalty) {
        Objects.requireNonNull(latencyBaseline, "latencyBaseline");
        if (latencyBaseline.isZero() || latencyBaseline.isNegative()) {
            throw new IllegalArgumentException("latencyBaseline must be positive");
        }
        if (!Double.isFinite(errorPenalty) || errorPenalty < 0) {
            throw new IllegalArgumentException("errorPenalty must be finite and >= 0");
        }
        this.latencyBaselineNanos = latencyBaseline.toNanos();
        this.errorPenalty = errorPenalty;
    }

    @Override
    public Backend select(List<Backend> candidates) {
        Objects.requireNonNull(candidates, "candidates");
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("candidates must not be empty");
        }

        int start = Math.floorMod(cursor.getAndIncrement(), candidates.size());
        Backend best = null;
        double bestScore = Double.POSITIVE_INFINITY;

        for (int offset = 0; offset < candidates.size(); offset++) {
            Backend candidate = candidates.get((start + offset) % candidates.size());
            double score = score(candidate);
            if (score < bestScore) {
                best = candidate;
                bestScore = score;
            }
        }
        return best;
    }

    /**
     * Exposes the score calculation for diagnostics and tests.
     */
    double score(Backend backend) {
        WindowSnapshot snapshot = backend.windowSnapshot();
        double weightedConnections = (backend.inFlightRequests() + 1.0) / backend.weight();
        double latencyFactor = 1.0 + snapshot.averageLatencyNanos() / latencyBaselineNanos;
        double errorFactor = 1.0 + snapshot.failureRate() * errorPenalty;
        return weightedConnections * latencyFactor * errorFactor;
    }
}
