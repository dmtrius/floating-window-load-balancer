package com.example.loadbalancer;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple round-robin selection, primarily useful as a baseline for comparing the
 * floating-window strategy.
 */
public final class RoundRobinStrategy implements SelectionStrategy {

    private final AtomicInteger cursor = new AtomicInteger();

    @Override
    public Backend select(List<Backend> candidates) {
        Objects.requireNonNull(candidates, "candidates");
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("candidates must not be empty");
        }
        int index = Math.floorMod(cursor.getAndIncrement(), candidates.size());
        return candidates.get(index);
    }
}
