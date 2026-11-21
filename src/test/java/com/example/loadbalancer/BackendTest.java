package com.example.loadbalancer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class BackendTest {

    @Test
    void rejectsInvalidConfigurationAndSamples() {
        Duration window = Duration.ofSeconds(1);
        assertThrows(IllegalArgumentException.class,
                () -> new Backend(" ", 1, window, 4));
        assertThrows(IllegalArgumentException.class,
                () -> new Backend("a", 0, window, 4));
        assertThrows(NullPointerException.class,
                () -> new Backend("a", 1, null, 4));

        Backend backend = new Backend("a", window, 4);
        assertThrows(IllegalArgumentException.class,
                () -> backend.recordSuccess(Duration.ofNanos(-1)));
        assertThrows(NullPointerException.class,
                () -> backend.recordFailure(null));
    }

    @Test
    void recordsManualOutcomesAndAvailability() {
        Backend backend = new Backend("a", 2, Duration.ofSeconds(1), 4);
        assertEquals(2, backend.weight());

        backend.recordSuccess(Duration.ofMillis(10));
        backend.recordFailure(Duration.ofMillis(30));

        assertEquals(2, backend.windowSnapshot().requests());
        assertEquals(1, backend.windowSnapshot().failures());
        assertEquals(20.0, backend.windowSnapshot().averageLatencyNanos() / 1_000_000.0, 1e-9);

        backend.setAvailable(false);
        assertFalse(backend.isAvailable());
        backend.setAvailable(true);
        assertTrue(backend.isAvailable());
    }
}
