package com.example.loadbalancer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class LoadBalancerTest {

    private static Backend backend(String id) {
        return new Backend(id, Duration.ofSeconds(10), 10);
    }

    @Test
    void executeRecordsTheSuccessfulOutcome() {
        Backend backend = backend("a");
        LoadBalancer loadBalancer = new LoadBalancer(List.of(backend));

        String result = loadBalancer.execute(selected -> {
            assertSame(backend, selected);
            return "ok";
        });

        assertEquals("ok", result);
        assertEquals(1, backend.windowSnapshot().requests());
        assertEquals(0, backend.windowSnapshot().failures());
        assertEquals(0, backend.inFlightRequests());
    }

    @Test
    void executeRecordsFailuresAndRethrowsTheOriginalException() {
        Backend backend = backend("a");
        LoadBalancer loadBalancer = new LoadBalancer(List.of(backend));
        RuntimeException boom = new RuntimeException("boom");

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> loadBalancer.execute(selected -> {
                    throw boom;
                }));

        assertSame(boom, thrown);
        assertEquals(1, backend.windowSnapshot().requests());
        assertEquals(1, backend.windowSnapshot().failures());
        assertEquals(0, backend.inFlightRequests());
    }

    @Test
    void selectSkipsUnavailableBackends() {
        Backend available = backend("a");
        Backend unavailable = backend("b");
        unavailable.setAvailable(false);
        LoadBalancer loadBalancer = new LoadBalancer(
                List.of(available, unavailable), new RoundRobinStrategy());

        for (int i = 0; i < 5; i++) {
            assertSame(available, loadBalancer.select());
        }
        assertEquals(0, available.inFlightRequests());
    }

    @Test
    void selectThrowsWhenEveryBackendIsUnavailable() {
        Backend backend = backend("a");
        backend.setAvailable(false);
        LoadBalancer loadBalancer = new LoadBalancer(List.of(backend));

        assertThrows(NoAvailableBackendException.class, loadBalancer::select);
    }

    @Test
    void inFlightRequestsSteerNewTrafficAwayFromTheBusyBackend() throws Exception {
        Backend a = backend("a");
        Backend b = backend("b");
        Backend c = backend("c");
        LoadBalancer loadBalancer = new LoadBalancer(List.of(a, b, c));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Backend> firstSelected = new AtomicReference<>();
        try {
            Future<?> held = executor.submit(() -> loadBalancer.execute(selected -> {
                firstSelected.set(selected);
                entered.countDown();
                awaitUninterruptibly(release);
                return null;
            }));

            assertTrue(entered.await(2, TimeUnit.SECONDS), "first request did not enter the backend");
            assertEquals(1, firstSelected.get().inFlightRequests());

            Backend second = loadBalancer.select();
            assertNotSame(firstSelected.get(), second);

            release.countDown();
            held.get(2, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }

        assertEquals(0, a.inFlightRequests());
        assertEquals(0, b.inFlightRequests());
        assertEquals(0, c.inFlightRequests());
    }

    @Test
    void leaseRecordsTheOutcomeAndReleasesTheSlotExactlyOnce() {
        Backend backend = backend("a");
        LoadBalancer loadBalancer = new LoadBalancer(List.of(backend));

        LoadBalancer.Lease lease = loadBalancer.acquire();
        try {
            assertSame(backend, lease.backend());
            assertEquals(1, backend.inFlightRequests());
            lease.failure();
            assertThrows(IllegalStateException.class, lease::success);
        } finally {
            lease.close();
        }

        lease.close();
        assertEquals(0, backend.inFlightRequests());
        assertEquals(1, backend.windowSnapshot().requests());
        assertEquals(1, backend.windowSnapshot().failures());
    }

    @Test
    void concurrentExecutionsDoNotLeakInFlightSlots() throws Exception {
        int taskCount = 120;
        List<Backend> backends = List.of(backend("a"), backend("b"), backend("c"));
        LoadBalancer loadBalancer = new LoadBalancer(backends);

        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < taskCount; i++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    loadBalancer.execute(selected -> {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("interrupted", ex);
                        }
                        return null;
                    });
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        long recorded = 0;
        for (Backend backend : backends) {
            assertEquals(0, backend.inFlightRequests());
            recorded += backend.windowSnapshot().requests();
        }
        assertEquals(taskCount, recorded);
    }

    @Test
    void constructorRejectsInvalidBackendSets() {
        assertThrows(IllegalArgumentException.class, () -> new LoadBalancer(List.of()));
        assertThrows(IllegalArgumentException.class, () -> new LoadBalancer(List.of(
                new Backend("same", Duration.ofSeconds(1), 4),
                new Backend("same", Duration.ofSeconds(1), 4))));
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", ex);
        }
    }
}
