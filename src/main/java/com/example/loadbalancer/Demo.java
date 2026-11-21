package com.example.loadbalancer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Small simulation that sends traffic to three backends with different speed and error
 * profiles. The slow/failing node receives progressively less traffic while its bad
 * observations are in the floating window, and the statistics disappear after the window
 * slides past them.
 */
public final class Demo {

    private static final Duration WINDOW = Duration.ofSeconds(3);
    private static final int BUCKETS = 6;
    private static final int REQUESTS = 150;

    public static void main(String[] args) throws InterruptedException {
        Backend fast = new Backend("api-fast", WINDOW, BUCKETS);
        Backend medium = new Backend("api-medium", WINDOW, BUCKETS);
        Backend slow = new Backend("api-slow", WINDOW, BUCKETS);
        List<Backend> backends = List.of(fast, medium, slow);

        LoadBalancer loadBalancer = new LoadBalancer(backends);
        Map<String, AtomicInteger> routed = new ConcurrentHashMap<>();
        for (Backend backend : backends) {
            routed.put(backend.id(), new AtomicInteger());
        }

        ExecutorService workers = Executors.newFixedThreadPool(8);
        CountDownLatch finished = new CountDownLatch(REQUESTS);
        for (int i = 0; i < REQUESTS; i++) {
            workers.submit(() -> {
                try {
                    loadBalancer.execute(backend -> {
                        routed.get(backend.id()).incrementAndGet();
                        simulate(backend.id());
                        return null;
                    });
                } catch (SimulatedFailure expectedFailure) {
                    // The backend window records the failure; the demo only counts routing.
                } finally {
                    finished.countDown();
                }
            });
        }

        if (!finished.await(20, TimeUnit.SECONDS)) {
            System.out.println("Timed out waiting for the simulated requests.");
        }
        workers.shutdown();
        workers.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("Routing counts for " + REQUESTS + " requests:");
        for (Backend backend : backends) {
            System.out.printf("  %-10s %d%n", backend.id(), routed.get(backend.id()).get());
        }

        System.out.println();
        System.out.println("Floating-window statistics:");
        printStats(backends);

        System.out.println();
        System.out.printf("Waiting %.1fs for the %.0fs window to slide...%n",
                WINDOW.toMillis() / 1000.0 + 0.2, WINDOW.toMillis() / 1000.0);
        Thread.sleep(WINDOW.toMillis() + 200);

        System.out.println("Statistics after the window has slid past every sample:");
        printStats(backends);
    }

    private static void printStats(List<Backend> backends) {
        for (Backend backend : backends) {
            WindowSnapshot snapshot = backend.windowSnapshot();
            System.out.printf("  %-10s requests=%3d failures=%3d avg=%6.1fms inFlight=%d%n",
                    backend.id(),
                    snapshot.requests(),
                    snapshot.failures(),
                    snapshot.averageLatencyNanos() / 1_000_000.0,
                    backend.inFlightRequests());
        }
    }

    private static void simulate(String backendId) {
        switch (backendId) {
            case "api-fast" -> sleep(8);
            case "api-medium" -> sleep(25);
            case "api-slow" -> {
                sleep(120);
                if (ThreadLocalRandom.current().nextDouble() < 0.35) {
                    throw new SimulatedFailure("simulated 503 from " + backendId);
                }
            }
            default -> throw new IllegalArgumentException("unknown backend " + backendId);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", ex);
        }
    }

    private static final class SimulatedFailure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private SimulatedFailure(String message) {
            super(message);
        }
    }
}
