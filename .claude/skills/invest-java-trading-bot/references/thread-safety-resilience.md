# Thread-safety and resilience — the deep dive

Read this when the task is about reliability, a bug involving duplicates/races/PnL drift, or making a
bot production-ready. The code patterns here are what separate a demo from a bot you can leave running.

## Where concurrency actually comes from

A running bot is a web of threads you don't fully control:

- **Stream consumers.** `MarketDataStreamManager.start()` launches one consumer loop per data type;
  listeners fire on those threads. Order-state and other server-side streams fire on their own
  gRPC threads.
- **gRPC transport threads** delivering responses to unary `CompletableFuture`s.
- **Your executors** — the pools you pass to the manager, the health-check scheduler, any workers.
- **The reconnect scheduler** inside the resilient wrappers, which re-subscribes asynchronously.

So a listener callback and a "place order" call can run simultaneously, and two candles can arrive
back-to-back on the same consumer thread before the first trade completes. Any state read-modify-written
without protection across those boundaries is a race.

## The layered model

Confine each concern to one thread, and share only through the channel between layers.

```
[ stream consumer thread(s) ]          [ your worker thread(s) ]
        |                                       ^
        v                                       |
   bounded queue  ──────────────────────►  decision engine        (serialised per instrument)
                                                |
                                                v
                                          OrderExecutor           (single choke point, idempotent)
                                                |
                                                v
                                          PositionTracker         (only owner of mutable positions)
```

Three properties make this safe:

1. **Exactly one thread mutates a given piece of trading state.** Positions are written only by
   `PositionTracker`, and it is driven from one consumer thread. If several threads must feed it, make
   the operations themselves atomic (`Map.merge`, `compute`, atomics) rather than lock around them.
2. **The network is isolated behind the executor.** Listeners never call the API; they enqueue.
   Workers never touch the stream; they drain.
3. **Backpressure is explicit.** The queue is bounded, so a market burst slows ingestion instead of
   growing memory without limit.

## Concrete patterns

### Per-instrument serialisation

The core bug this prevents: two events for the same instrument both observe "no open position" and
both send a buy. The fix is that decisions for one instrument happen on one thread *in order*.

```java
// One worker per instrument key keeps ordering; distinct instruments still run in parallel.
private final ConcurrentHashMap<String, ExecutorService> instrumentExecutors = new ConcurrentHashMap<>();

void onCandle(CandleWrapper candle) {
  var exec = instrumentExecutors.computeIfAbsent(
      candle.getInstrumentUid(),
      k -> Executors.newSingleThreadExecutor(r -> {
        var t = new Thread(r, "decide-" + k);
        t.setDaemon(true);
        return t;
      }));
  exec.execute(() -> strategy.onBar(candle));   // ordered per instrument, parallel across
}
```

If you only have a handful of instruments, a single shared decision thread is even simpler and just
as correct — concurrency *across* instruments is the win; concurrency *within* one is the hazard.

### Bounded hand-off, explicit rejection

```java
private final BlockingQueue<CandleWrapper> queue = new ArrayBlockingQueue<>(10_000);

OnNextListener<CandleWrapper> listener = candle -> {
  if (!queue.offer(candle)) {
    metrics.incrementDropped();
    log.warn("queue full, dropped candle for {}", candle.getInstrumentUid());
    // deliberate: dropping a stale candle beats blocking the stream and backing up every subscription
  }
};

// worker
while (running) {
  var candle = queue.poll(200, TimeUnit.MILLISECONDS);
  if (candle != null) engine.onCandle(candle);
}
```

Choose the rejection policy deliberately. For market data, dropping-and-counting is usually right —
you can reconstruct a missed candle from history. For **orders you must never drop**; never put order
submission behind a lossy queue.

### Atomic position updates

`ConcurrentHashMap` gives you atomic compound operations — use them instead of get-then-put:

```java
// realised PnL accumulation without a lost update
private final ConcurrentHashMap<String, BigDecimal> realisedPnl = new ConcurrentHashMap<>();

void onFill(String instrumentId, BigDecimal signedAmount) {
  realisedPnl.merge(instrumentId, signedAmount, BigDecimal::add);   // atomic
}

// position lifecycle with a conditional remove
positions.compute(instrumentId, (k, current) -> {
  if (current == null) return null;
  var next = current.subtract(soldAmount);
  return next.signum() <= 0 ? null : next;      // flat -> remove entry
});
```

`AtomicReference` for a swappable snapshot, `LongAdder` for high-throughput counters (metrics),
`AtomicBoolean` for the running flag. Reach for `synchronized` only when a compound operation spans
several structures — and keep the critical section free of I/O.

### Idempotent execution

Every order needs a key that survives retries and reconnects:

```java
// generated once, before the call that may be retried
String orderId = UUID.randomUUID().toString();
pending.put(orderId, intent);

try {
  orders.callSyncMethod(stub -> stub.postOrderAsync(requestWith(orderId)));
} catch (ServiceRuntimeException e) {
  // do NOT mint a new id and resend — reconcile with the SAME id
  reconcile(orderId);
}
```

Track `orderId -> intent` so a reconnect can ask "which of my orders actually filled?" and never
resubmit an intent that already executed. This is the difference between a flaky network causing a
retry and it causing a doubled position.

### Executors: separate by purpose, own their lifetime

```java
private final ExecutorService streamingPool  = Executors.newCachedThreadPool();      // stream consumers
private final ScheduledExecutorService healthPool =
    Executors.newSingleThreadScheduledExecutor();                                     // reconnects/pings
private final ExecutorService tradingPool    = Executors.newFixedThreadPool(2);       // order workers
```

- The manager takes the first two; keep the scheduled pool **separate and single-threaded** so a
  saturated streaming pool cannot delay the health-check that detects a dead stream.
- Give threads names and set daemons where appropriate — it makes production thread dumps readable.
- You own every executor you create. Shut them down in `stop()`; `MarketDataStreamManager.shutdown()`
  does **not** shut down the pools you passed it.

### Clean lifecycle

```java
public void stop() {
  running.set(false);
  manager.shutdown();          // disconnects streams
  orderStateWrapper.disconnect();
  tradingPool.shutdown();
  streamingPool.shutdown();
  healthPool.shutdown();
  awaitTermination(tradingPool);   // let in-flight orders settle
  log.info("bot stopped");
}

public void start() {
  Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "bot-shutdown"));
  manager.start();
}
```

Register the hook before `start()`. A bot killed with subscriptions still open leaves server-side
state you'll have to reason about on the next launch.

## Failure catalogue

| Symptom | Likely cause | Fix |
|---|---|---|
| Positions wrong after the app ran overnight | Reconnect gap not reconciled; fill events missed | Reconcile from `getPortfolio` in the order-state stream's `addOnConnectListener(Runnable)` plus a periodic timer (the manager's own connect hook is `protected`) |
| Duplicate orders on a flaky connection | New order id generated on retry | Generate the id once, reuse it, reconcile by id |
| Bot stops receiving data, no error | A listener threw: the manager's consumer loop dies silently (only `InterruptedException` is caught; `submit()` swallows the rest) and the healthy gRPC stream never triggers reconnect | try/catch `Throwable` in every listener; add a "no candles for N intervals" watchdog; check thread dump for a missing consumer thread |
| Bot stops receiving data, no error | Subscription lost after reconnect; or `start()` never called | Use resilient wrapper; verify `start()`; check `isSubscribed*` |
| Everything stalls under load | Blocking call (DB write / unary) inside a listener | Move the work to a queue + worker |
| Occasional double buy, hard to reproduce | Two threads decide for the same instrument concurrently | Serialise decisions per instrument |
| Every candle processed twice, double orders | `subscribe*` called twice with the same listener: the manager's dedupe compares by reference against a fresh wrapper lambda, so the listener is registered twice | Subscribe exactly once (guard with `isSubscribed*`/a flag); dedupe per candle on instrument + `getTime()`; deterministic order id derived from instrument + candle time + direction |
| `RESOURCE_EXHAUSTED` storms | Unbounded request rate | Rate limiter in `ResilienceConfiguration`; honour `getRateLimitReset()` |
| Memory climbs until OOM | Unbounded queue under a slow consumer | `ArrayBlockingQueue` with a rejection policy + metrics |
| Health-check/reconnect never fires | Scheduled pool starved by other work, or a connect listener threw/blocked inside `healthCheck` (`scheduleAtFixedRate` cancels all later runs after an exception) | Dedicated single-threaded scheduled pool; connect listeners only `try { pool.execute(...) } catch (Throwable)` |
| Half-open connection: no `onError`, no `onComplete`, no data | Channel sets `keepAliveTimeout` but not `keepAliveTime`, so no HTTP/2 pings; only the SDK's application-level ping/inactivity check (resilient wrappers) notices | Use resilient wrappers; add a freshness watchdog on the last received payload; never rely on `onError` alone |

## Resilience, reassessed

- The SDK's **default retry already covers the right cases** for the API (`RESOURCE_EXHAUSTED`,
  `UNAVAILABLE`, `INTERNAL`/70001). Add circuit breaker / rate limiter / bulkhead when you have a
  specific need; don't stack a hand-rolled retry on top.
- **Circuit breaker** protects you when the API is degraded — fail fast and stop hammering, rather
  than queuing thousands of doomed calls.
- **Rate limiter** is the right tool for "I must not exceed N calls/sec" (parallel order bursts,
  wide instrument scans).
- **Bulkhead** caps concurrent in-flight calls and keeps one slow call path from consuming all threads.
- Do **not** retry `INVALID_ARGUMENT`, `FAILED_PRECONDITION`, `NOT_FOUND`, `PERMISSION_DENIED` —
  they are deterministic; retrying only wastes quota and delays the real error.

## Testing the concurrency

- Drive the decision engine directly with a `BarSeries`/candle sequence — no network — and assert
  signals. This is why `StrategyEngine` is pure.
- Test position updates with interleaved calls from several threads and assert the invariant
  (never negative quantity, sum of deltas equals final position).
- Simulate reconnect by calling the connect listener twice and asserting reconciliation is idempotent.
- `core` tests use `grpcmock-junit5` to stand up an in-process gRPC server; use it to test the
  executor/streaming layers without hitting the real API.
