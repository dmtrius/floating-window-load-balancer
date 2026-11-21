package com.example.loadbalancer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class FloatingWindowTest {

    private final AtomicLong clock = new AtomicLong();

    private FloatingWindow window(Duration duration, int buckets) {
        return new FloatingWindow(duration, buckets, clock::get);
    }

    @Test
    void aggregatesRequestsFailuresAndLatency() {
        FloatingWindow window = window(Duration.ofSeconds(1), 4);

        window.record(true, 10_000_000L);
        clock.addAndGet(300_000_000L);
        window.record(false, 30_000_000L);

        WindowSnapshot snapshot = window.snapshot();
        assertEquals(2, snapshot.requests());
        assertEquals(1, snapshot.failures());
        assertEquals(40_000_000L, snapshot.latencyNanos());
        assertEquals(0.5, snapshot.failureRate(), 1e-12);
        assertEquals(20_000_000.0, snapshot.averageLatencyNanos(), 1e-9);
        assertEquals(Duration.ofSeconds(1), snapshot.window());
    }

    @Test
    void expiresObservationsOnceTheyLeaveTheWindow() {
        FloatingWindow window = window(Duration.ofSeconds(1), 4);
        window.record(true, 10_000_000L);

        clock.set(900_000_000L);
        assertEquals(1, window.snapshot().requests(), "observation should still overlap the window");

        clock.set(1_000_000_000L);
        assertEquals(0, window.snapshot().requests(), "observation should have aged out");
    }

    @Test
    void ignoresFutureBucketsWhenClockMovesBackwards() {
        FloatingWindow window = window(Duration.ofSeconds(1), 4);
        clock.set(1_000_000_000L);
        window.record(true, 5_000_000L);

        clock.set(0L);
        assertEquals(0, window.snapshot().requests());
    }

    @Test
    void emptyWindowHasZeroRates() {
        WindowSnapshot snapshot = window(Duration.ofSeconds(2), 10).snapshot();
        assertEquals(0, snapshot.requests());
        assertEquals(0.0, snapshot.failureRate());
        assertEquals(0.0, snapshot.averageLatencyNanos());
        assertEquals(0.0, snapshot.requestsPerSecond());
    }

    @Test
    void clearDropsAllObservations() {
        FloatingWindow window = window(Duration.ofSeconds(1), 4);
        window.record(false, 5_000_000L);
        window.clear();

        assertEquals(0, window.snapshot().requests());
    }

    @Test
    void rejectsInvalidConfigurationAndSamples() {
        assertThrows(IllegalArgumentException.class,
                () -> new FloatingWindow(Duration.ZERO, 4));
        assertThrows(IllegalArgumentException.class,
                () -> new FloatingWindow(Duration.ofSeconds(1), 0));
        assertThrows(IllegalArgumentException.class,
                () -> new FloatingWindow(Duration.ofNanos(3), 4));

        FloatingWindow window = window(Duration.ofSeconds(1), 4);
        assertThrows(IllegalArgumentException.class, () -> window.record(true, -1));
    }
}
