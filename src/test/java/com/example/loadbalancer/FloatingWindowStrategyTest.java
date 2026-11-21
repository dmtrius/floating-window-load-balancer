package com.example.loadbalancer;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class FloatingWindowStrategyTest {

    private static Backend backend(String id) {
        return new Backend(id, Duration.ofSeconds(10), 10);
    }

    @Test
    void equalBackendsRotateRoundRobin() {
        FloatingWindowStrategy strategy = new FloatingWindowStrategy();
        Backend a = backend("a");
        Backend b = backend("b");
        Backend c = backend("c");
        List<Backend> candidates = List.of(a, b, c);

        assertSame(a, strategy.select(candidates));
        assertSame(b, strategy.select(candidates));
        assertSame(c, strategy.select(candidates));
        assertSame(a, strategy.select(candidates));
    }

    @Test
    void avoidsBackendWithRecentFailures() {
        FloatingWindowStrategy strategy = new FloatingWindowStrategy();
        Backend a = backend("a");
        Backend failing = backend("failing");
        Backend c = backend("c");
        for (int i = 0; i < 5; i++) {
            failing.recordFailure(Duration.ofMillis(1));
        }

        List<Backend> candidates = List.of(a, failing, c);
        for (int i = 0; i < 30; i++) {
            assertNotSame(failing, strategy.select(candidates));
        }
    }

    @Test
    void avoidsBackendWithRecentHighLatency() {
        FloatingWindowStrategy strategy = new FloatingWindowStrategy();
        Backend fast = backend("fast");
        Backend slow = backend("slow");
        slow.recordSuccess(Duration.ofMillis(500));

        List<Backend> candidates = List.of(fast, slow);
        for (int i = 0; i < 10; i++) {
            assertSame(fast, strategy.select(candidates));
        }
    }

    @Test
    void weightAdjustsTheConcurrencyPenalty() {
        FloatingWindowStrategy strategy = new FloatingWindowStrategy();
        Backend normal = new Backend("normal", 1, Duration.ofSeconds(10), 10);
        Backend heavy = new Backend("heavy", 4, Duration.ofSeconds(10), 10);

        normal.beginRequest();
        heavy.beginRequest();
        heavy.beginRequest();
        heavy.beginRequest();
        try {
            assertTrue(strategy.score(heavy) < strategy.score(normal));
            assertSame(heavy, strategy.select(List.of(normal, heavy)));
        } finally {
            normal.endRequest();
            heavy.endRequest();
            heavy.endRequest();
            heavy.endRequest();
        }
    }

    @Test
    void rejectsEmptyCandidatesAndInvalidConfiguration() {
        FloatingWindowStrategy strategy = new FloatingWindowStrategy();
        assertThrows(IllegalArgumentException.class, () -> strategy.select(List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new FloatingWindowStrategy(Duration.ZERO, 1.0));
        assertThrows(IllegalArgumentException.class,
                () -> new FloatingWindowStrategy(Duration.ofMillis(10), -1.0));
        assertThrows(IllegalArgumentException.class,
                () -> new FloatingWindowStrategy(Duration.ofMillis(10), Double.NaN));
    }
}
