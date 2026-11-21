# Floating-Window Load Balancer (Java 17)

A small, dependency-free, thread-safe load balancer that chooses backends using a
**floating (sliding) time window** of recent latency and failure observations.

> **Interpretation:** "float window" is implemented as a time-based sliding window
> (a ring of fixed-duration buckets), not a GUI window. If you meant a desktop windowing
> feature or a different balancing algorithm, the strategy interface is the extension point.

## What it does

Each backend owns:

- a `FloatingWindow` containing recent request count, failures, and latency;
- a live in-flight request counter;
- an availability flag and a capacity weight.

For every request, `FloatingWindowStrategy` scores each available backend and picks the
lowest score:

```text
score = ((inFlight + 1) / weight)
      * (1 + averageLatency / latencyBaseline)
      * (1 + failureRate * errorPenalty)
```

- **Concurrency** is an immediate signal: busy nodes score higher.
- **Latency and errors** come from the floating window: a slow or flaky node gets less
  traffic while the bad observations are in the window.
- **Recovery is automatic**: old samples fall out of the window, so a recovered node becomes
  eligible again without an operator reset.
- **Weights** divide the concurrency penalty, so a weight-2 node can absorb twice the
  in-flight work at the same score.
- **Ties** are broken round-robin for fair distribution.

Defaults are a 50 ms latency baseline and an error penalty of 10. Both are constructor
parameters. Bucket count and window length are configured per backend.

## Floating window mechanics

`FloatingWindow` stores observations in a fixed ring of buckets. There is no eviction thread
and no timer reset. At snapshot time it sums only the buckets covering the most recent
window:

```text
bucket 0   bucket 1   bucket 2   ...   bucket N-1
|----------|----------|------------------|----------|
              <------ current window ------> now
```

- Recording a sample is `O(1)`.
- Taking a snapshot is `O(bucketCount)` (typically 10 buckets).
- Memory is `O(backends * bucketCount)`.
- Aggregation is approximate to one bucket at the window edge; this is the standard
  fixed-bucket trade-off described in the Javadoc.

## Quick start

Requirements: JDK 17+ and Maven 3.9+.

```bash
mvn test
mvn package
java -cp target/classes com.example.loadbalancer.Demo
```

The demo sends 150 concurrent requests to three simulated backends:

```text
Routing counts for 150 requests:
  api-fast   114
  api-medium 32
  api-slow   4

Floating-window statistics:
  api-fast   requests=114 failures=  0 avg=   8.7ms inFlight=0
  api-medium requests= 32 failures=  0 avg=  25.8ms inFlight=0
  api-slow   requests=  4 failures=  1 avg= 122.8ms inFlight=0
```

After waiting for the window to slide, all counters return to zero. Exact counts vary
between runs because simulated failures are random.

## Using it

### Synchronous execution

```java
Backend a = new Backend("node-a", 1, Duration.ofSeconds(10), 10);
Backend b = new Backend("node-b", 2, Duration.ofSeconds(10), 10);

LoadBalancer loadBalancer = new LoadBalancer(List.of(a, b));

Response response = loadBalancer.execute(backend ->
        httpClient.send(request, backend.id()));
```

`execute` increments the selected backend's in-flight count, measures the call, records
success or failure in its window, and always releases the slot. Runtime exceptions and
errors are recorded as failures and rethrown unchanged. There are no implicit retries.

### Asynchronous / long-lived work

```java
try (LoadBalancer.Lease lease = loadBalancer.acquire()) {
    try {
        callAsync(lease.backend());
        lease.success();
    } catch (RuntimeException ex) {
        lease.failure();
        throw ex;
    }
}
```

`acquire()` reserves an in-flight slot up front. Record success or failure once, then
close the lease to release the slot. The lease is safe to close more than once.

### Manual availability control

```java
backend.setAvailable(false); // drain / take out of rotation
backend.setAvailable(true);  // put back
```

This project does not run active health checks; connect it to your own probe or
orchestrator and call `setAvailable`.

### Custom strategy

`SelectionStrategy` has a single method and receives the currently available candidates:

```java
LoadBalancer loadBalancer = new LoadBalancer(backends, candidates -> candidates.get(0));
```

`RoundRobinStrategy` is included as a baseline for comparison.

## Project layout

```text
src/main/java/com/example/loadbalancer/
  Backend.java                 backend identity, weight, window, in-flight count
  FloatingWindow.java          ring of time buckets and snapshot aggregation
  WindowSnapshot.java          immutable per-window aggregate
  SelectionStrategy.java       strategy contract
  FloatingWindowStrategy.java  latency/error/in-flight scoring
  RoundRobinStrategy.java      baseline strategy
  LoadBalancer.java            availability filter, execution lifecycle, leases
  NoAvailableBackendException.java
  Demo.java                    runnable simulation

src/test/java/com/example/loadbalancer/
  FloatingWindowTest.java      boundary, expiry, validation
  FloatingWindowStrategyTest.java  scoring, weights, tie-breaking
  LoadBalancerTest.java        lifecycle, concurrency, leases
  BackendTest.java             backend validation and manual metrics
```

## Design scope and limitations

- Single-JVM library; it does not replicate state across load-balancer instances.
- Passive only: no background health probes. Unavailable nodes must be marked explicitly.
- No retries or request hedging; failures surface to the caller.
- One score is calculated per candidate from separate in-flight and window reads, so it is
  a near-real-time signal rather than a serializable distributed snapshot.
- HTTP transport is intentionally not included; `execute` is transport-agnostic.

## Tests

The suite covers:

- bucket aggregation, window expiry, clock rollback, and input validation;
- round-robin tie-breaking and avoiding recently slow/failing nodes;
- weight-adjusted in-flight scoring;
- success/failure recording, lease semantics, unavailable-node filtering, exceptions;
- 120 concurrent executions with no leaked in-flight slots.

All 21 tests pass.
