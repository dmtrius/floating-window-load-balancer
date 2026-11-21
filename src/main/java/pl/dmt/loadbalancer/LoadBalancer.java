package pl.dmt.loadbalancer;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A transport-agnostic load balancer over a fixed set of {@link Backend}s.
 *
 * <p>Selection is delegated to a {@link SelectionStrategy}; the default strategy is
 * {@link FloatingWindowStrategy}. The balancer adds the lifecycle work around selection:
 * it filters unavailable nodes, tracks in-flight requests, measures latency, and records
 * success or failure in the selected backend's floating window.
 *
 * <p>The class is thread-safe. It does not perform retries or active health checks; those are
 * policy decisions that belong to the caller.
 */
public final class LoadBalancer {

    private final List<Backend> backends;
    private final SelectionStrategy strategy;

    /** Creates a balancer using {@link FloatingWindowStrategy}. */
    public LoadBalancer(Collection<Backend> backends) {
        this(backends, new FloatingWindowStrategy());
    }

    /**
     * @param backends non-empty collection of backends with unique ids
     * @param strategy thread-safe selection strategy
     */
    public LoadBalancer(Collection<Backend> backends, SelectionStrategy strategy) {
        Objects.requireNonNull(backends, "backends");
        Objects.requireNonNull(strategy, "strategy");
        if (backends.isEmpty()) {
            throw new IllegalArgumentException("backends must not be empty");
        }

        List<Backend> copy = List.copyOf(backends);
        Set<String> ids = new HashSet<>();
        for (Backend backend : copy) {
            if (!ids.add(backend.id())) {
                throw new IllegalArgumentException("duplicate backend id: " + backend.id());
            }
        }
        this.backends = copy;
        this.strategy = strategy;
    }

    /** Immutable view of all configured backends, including unavailable ones. */
    public List<Backend> backends() {
        return backends;
    }

    /**
     * Selects one available backend without changing its in-flight count.
     *
     * @throws NoAvailableBackendException if every backend is marked unavailable
     */
    public Backend select() {
        List<Backend> candidates = backends.stream()
                .filter(Backend::isAvailable)
                .toList();
        if (candidates.isEmpty()) {
            throw new NoAvailableBackendException(
                    "none of the " + backends.size() + " backends is available");
        }

        Backend selected = strategy.select(candidates);
        if (selected == null || !candidates.contains(selected)) {
            throw new IllegalStateException("selection strategy returned an invalid backend: " + selected);
        }
        return selected;
    }

    /**
     * Selects a backend, invokes {@code request} with it, and records the outcome in the
     * backend's floating window. The in-flight count is incremented before the call and
     * decremented in a {@code finally} block.
     *
     * <p>Runtime exceptions and errors are recorded as failures and rethrown unchanged.
     * There is no retry: a failed request is visible to the caller.
     */
    public <T> T execute(Function<? super Backend, ? extends T> request) {
        Objects.requireNonNull(request, "request");

        Backend backend = select();
        backend.beginRequest();
        long startedAt = System.nanoTime();
        try {
            final T result;
            try {
                result = request.apply(backend);
            } catch (RuntimeException | Error ex) {
                backend.recordOutcome(false, System.nanoTime() - startedAt);
                throw ex;
            }
            backend.recordOutcome(true, System.nanoTime() - startedAt);
            return result;
        } finally {
            backend.endRequest();
        }
    }

    /**
     * Acquires a backend for work that cannot be expressed as a synchronous
     * {@link #execute(Function)} call, such as asynchronous I/O.
     *
     * <p>The returned lease increments the in-flight count immediately. Call
     * {@link Lease#success()} or {@link Lease#failure()} exactly once to record the outcome,
     * then close the lease to decrement the in-flight count:
     *
     * <pre>{@code
     * try (LoadBalancer.Lease lease = loadBalancer.acquire()) {
     *     try {
     *         callAsync(lease.backend());
     *         lease.success();
     *     } catch (RuntimeException ex) {
     *         lease.failure();
     *         throw ex;
     *     }
     * }
     * }</pre>
     */
    public Lease acquire() {
        Backend backend = select();
        backend.beginRequest();
        return new Lease(backend);
    }

    /**
     * A reservation of one in-flight slot on a backend.
     */
    public static final class Lease implements AutoCloseable {

        private final Backend backend;
        private final long startedAtNanos;
        private boolean outcomeRecorded;
        private boolean closed;

        private Lease(Backend backend) {
            this.backend = backend;
            this.startedAtNanos = System.nanoTime();
        }

        public Backend backend() {
            return backend;
        }

        /** Records success. The outcome can be recorded only once, before the lease is closed. */
        public synchronized void success() {
            record(true);
        }

        /** Records failure. The outcome can be recorded only once, before the lease is closed. */
        public synchronized void failure() {
            record(false);
        }

        private void record(boolean success) {
            if (closed) {
                throw new IllegalStateException("lease is already closed");
            }
            if (outcomeRecorded) {
                throw new IllegalStateException("outcome has already been recorded");
            }
            outcomeRecorded = true;
            backend.recordOutcome(success, System.nanoTime() - startedAtNanos);
        }

        /** Releases the in-flight slot. The lease can be closed more than once. */
        @Override
        public synchronized void close() {
            if (!closed) {
                closed = true;
                backend.endRequest();
            }
        }
    }
}
