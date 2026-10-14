---
name: invest-java-trading-bot
description: Build or modify trading bots (торговые роботы) on the T-Invest API Java SDK (ru.tinkoff.piapi / ru.ttech.piapi, Т-Инвестиции, invest-java). Use this skill whenever the user works with this SDK — creating a new bot, changing an existing one, subscribing to market-data streams (candles, trades, order book, last price, trading status) via MarketDataStreamManager, placing or cancelling orders, using the Spring Boot starter (CandleTradingBot) or the ta4j strategy module, backtesting, or asking about ConnectorConfiguration, resilience4j setup, auto-reconnect, duplicate orders, or thread-safety in a bot. Trigger even when the user only describes bot behavior ("робот должен покупать по пересечению MA") without naming the SDK, and even for a narrow question like "why does my stream lose the subscription" or "почему бот перестал получать свечи". Covers the SDK's own abstractions plus Java concurrency and resilience best practices for trading systems.
license: Apache-2.0
---

# Building trading bots on the T-Invest Java SDK

This skill helps produce bots that are **correct under concurrency**, **resilient to the API's
failure modes**, and **wired to the right level of the SDK** — not just bots that compile and
place an order.

The three failure modes that matter in production, in order of how often they bite:

1. **Concurrency on shared state.** A bot is a stream of callbacks arriving on gRPC threads,
   plus occasional REST-style unary calls, touching shared positions and orders. Unsynchronised
   state here produces double orders and wrong PnL.
2. **Silent stream death.** Market-data streams drop, subscriptions get lost, and positions
   drift from reality after a reconnect. The SDK has auto-reconnect wrappers; hand-rolled
   reconnects are almost always worse.
3. **Wrong abstraction level.** Hand-rolling a gRPC stub or a retry loop when the SDK already
   has a tested wrapper for exactly that.

Read the relevant reference file before writing code; do not guess SDK signatures.

## Step 1 — Understand the task and pick the abstraction level

Bots in this ecosystem fall into three shapes. Getting this decision right saves the rest of
the work.

| Shape | Use when | Entry point |
|---|---|---|
| **Spring Boot starter** | The user wants a bot in a Spring app, or wants the SDK to handle warmup, subscription and dispatch | implement `CandleTradingBot` (`spring-boot/`) |
| **Strategy module** | Candle/ta4j strategy, backtest or live, outside Spring | `StrategyFactory` → `CandleStrategy` (`strategy/`) |
| **Core, hand-wired** | Custom data (order book, trades, last price), custom execution, multi-account, or anything the two above don't cover | `ServiceStubFactory` + `StreamServiceStubFactory` + `StreamManagerFactory` (`core/`) |

Ask (or infer from context): which data does the bot need? which signals? which execution?
If candles + ta4j cover it, the strategy module or the starter is the answer — reach for raw
core only when the requirement genuinely falls outside them.

When **modifying** an existing bot, first locate which shape it is, then stay within that shape
unless the user asks for a rewrite. Read the code before proposing changes.

## Step 2 — Structure the bot in layers

Whatever the shape, keep these responsibilities separate. This is the single biggest lever on
both thread-safety and testability, because it confines I/O and mutable state to known places.

```
MarketDataGateway   owns the stream manager + listeners; owns no trading logic
StrategyEngine      pure: candle/tick in -> signal out; no I/O, no shared state
OrderExecutor       the only place that talks to Orders/Sandbox/Operations services
PositionTracker     the only place that owns position/PnL mutable state
BotLifecycle        start(), stop(), shutdown hook; owns executor lifetimes
```

Rules that make the layers real:
- **StrategyEngine must be pure.** Given a bar series it returns entry/exit/do-nothing. This
  makes it unit-testable and removes the temptation to hit the network from a callback.
- **Only OrderExecutor places orders.** One choke point means one place to serialise, log,
  rate-limit and make idempotent.
- **Only PositionTracker mutates positions.** Funnel every update — fills, stream events,
  reconnects — through it.

## Step 3 — Wire the connector

The construction chain is fixed; each link takes the previous one:

```java
var configuration = ConnectorConfiguration.loadPropertiesFromFile("invest.properties");
var stubs        = ServiceStubFactory.create(configuration);
var streamStubs  = StreamServiceStubFactory.create(stubs);
var manager      = StreamManagerFactory.create(streamStubs)
    .newMarketDataStreamManager(executor, scheduledExecutor);
```

Key facts to rely on (full detail in `references/core-api.md`):
- There are **no typed stub getters**. You create each service by passing the generated stub
  constructor: `stubs.newSyncService(OrdersServiceGrpc::newBlockingStub)`.
- `ServiceStubFactory` owns **one shared `ManagedChannel`** and attaches auth + error
  interceptors. Never construct a gRPC stub directly — you would lose the token and the
  interceptors.
- `ConnectorConfiguration` is immutable and safe to share across threads. It has **no builder** —
  only `loadFromProperties*` static factories.
- Every unary call throws `ServiceRuntimeException` (unchecked) on failure. That is the class
  to catch; it carries `getTrackingId()`, `parseErrorCode()` and `getRateLimitReset()`.
- **The module READMEs have drifted from the code.** They show `loadFromPropertiesFile(...)` (the real
  methods are `loadPropertiesFromFile` / `loadPropertiesFromResources`) and a one-argument
  `newMarketDataStreamManager(executor)` (the real one takes `(ExecutorService, ScheduledExecutorService)`).
  Copying README snippets verbatim produces code that does not compile; check names against
  `references/core-api.md` or the source.

## Step 4 — Subscribe to market data

Prefer `MarketDataStreamManager` over a bare stream. It distributes subscriptions across
streams to respect the API's per-stream subscription limits, so the bot keeps working as the
instrument set grows. Full signatures in `references/streaming.md`.

```java
manager.subscribeCandles(
    Set.of(new Instrument(uid, SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)),
    new CandleSubscriptionSpec(),          // waitingClose = true by default
    candle -> gateway.publish(candle));    // do NOT block or trade here
manager.start();                           // easy to forget; nothing is delivered without it
```

Two habits that prevent most stream bugs:
- **Never block in a listener.** Listeners run on the manager's consumer threads; a blocking
  call there stalls delivery for *every* subscription. Hand the payload to a bounded queue
  and let a worker thread act on it.
- **Subscribe once; never re-subscribe "to be safe".** `subscribe*` wraps your listener in a fresh
  filtering lambda each call, and the manager's duplicate check (`listeners.contains`) compares by
  reference, so it never matches. Calling `subscribeCandles` a second time with the same instruments
  and the same listener registers the listener *twice* and every candle is delivered twice — a direct
  route to double orders. The resilient wrappers already resubscribe on reconnect, so do not add your
  own re-subscribe in a connect handler or retry loop. Guard with `isSubscribedCandles(...)` / a
  `started` flag, and make decisions idempotent per candle (dedupe on instrument + candle time).
- **Log stream errors and completion — where the SDK lets you.** A stream that ends or fails
  produces no output unless something listens. Which hook exists depends on the layer:
  - **Raw streams** (`newServerSideStream`, `newBidirectionalStream`, `MarketDataStreamConfiguration`):
    always register `addOnErrorListener(e -> log.error("stream failed", e))` and
    `addOnCompleteListener(() -> log.warn("stream completed"))`. Without them a server-side close
    (deploy, rotation, idle limit) is invisible, and a raw stream never reconnects by itself.
  - **Resilient wrappers and `MarketDataStreamManager`** expose **no** error/complete listeners — their
    builders only have data and connect hooks. The SDK also does not log stream errors itself, so
    a failed stream is recovered silently, and only after `stream.inactivity-timeout` (15 s by
    default) of silence. You will see the recovery as `Wrapper ... reconnecting...` at INFO in the
    `ru.ttech.piapi` logger, so keep that logger at INFO or lower. Count reconnects and alert when they
    become frequent; frequent reconnects mean a network or token problem that the wrapper is masking.
- **Never let an exception escape a listener.** The manager's consumer loop
  (`startListenersProcessing`) catches only `InterruptedException`, and it runs under
  `executorService.submit`, which stores any other exception in an ignored `Future`. One uncaught
  `RuntimeException` therefore ends the consumer for that data type *silently*: the gRPC stream stays
  healthy, so the reconnect logic never fires, and callbacks just stop with nothing in the log. Wrap
  every listener body in `try { ... } catch (Throwable t) { log.error(...) }` (the SDK's own
  `CandleStrategy` does this for enter/exit actions). Also, listeners for one data type are called in
  a `forEach`, so a throwing listener starves the ones registered after it.
- **Reconcile on every (re)connect — but not on the connect thread.** A reconnect leaves the bot
  trading against stale state — the source of most "the bot duplicated a position overnight" bugs.
  Where you can hook the event depends on the stream:
  - **Order-state / trades / portfolio / positions streams** (the ones that carry *your* fills):
    `addOnConnectListener(Runnable)` on the `*WrapperConfiguration` builder is public. Reconcile
    here — this is the stream whose events are lost during a gap.
  - **Market data via `MarketDataStreamManager`**: its `addOnConnectListener` is `protected`, so bot
    code cannot call it directly. Usually you do not need it: market-data gaps are repaired by
    `MarketDataManager` (backfill, `isHistory() == true`) and positions do not depend on market-data
    events, so reconcile via the order-state stream's connect hook plus a periodic timer (e.g. every
    60 s). If you really need the market-data reconnect event, the constructor is public, so a thin
    subclass can expose the hook (register it *before* the first `subscribe*`, because listeners are
    copied into each stream wrapper when it is created):
    `class BotStreamManager extends MarketDataStreamManager { void onConnect(OnConnectListener<OnConnectEvent> l) { addOnConnectListener(l); } }`.
    That leans on SDK internals and can break on upgrade, so treat it as a last resort.
  - **A hand-built `MarketDataStreamWrapper`**: `MarketDataStreamWrapperConfiguration.builder(...)
    .addOnConnectListener(OnConnectListener<OnConnectEvent>)` is public.

  The SDK invokes connect listeners *inside* its `healthCheck`, which runs under `scheduleAtFixedRate`
  on your scheduled pool: if the listener throws, every later health-check is silently cancelled, and
  if it blocks on unary calls the health-check stalls. So the listener body should be
  `try { reconcilePool.execute(this::reconcile); } catch (Throwable t) { log.error(...); }` — hand off
  to a separate pool and never let anything escape.

## Step 5 — Enforce thread-safety

The concurrency rules that matter for a bot. Rationale and code in
`references/thread-safety-resilience.md`.

- **Serialise decisions per instrument.** Two candles for the same instrument processed
  concurrently will both see "flat" and both buy. Route all decision-making for one instrument
  through a single thread (one worker per instrument key, or one consumer thread for all —
  either is fine, concurrency across instruments is what matters).
- **Share mutable state only through concurrent collections or atomics.**
  `ConcurrentHashMap`, `AtomicReference`, `AtomicLong`, `LongAdder`. No plain `HashMap`/`double`
  touched from a listener.
- **Money is `BigDecimal`.** The SDK's wrapper types already convert `Quotation`/`MoneyValue` to
  `BigDecimal`; keep them that way. Never `double` for price or quantity.
- **Bound every queue.** An unbounded queue in front of a slow consumer is an OOM waiting for a
  busy market. `ArrayBlockingQueue` with an explicit capacity and a defined rejection policy.
- **Separate executor pools by purpose.** A cached pool for stream consumers, a
  **single-threaded scheduled** pool for health-checks/reconnects. Sharing one pool lets a busy
  consumer delay the health-check that would have caught a dead stream. The consumer pool must
  have at least 6 threads available: `start()` submits six never-ending loops (one per data type),
  so a fixed pool smaller than that silently starves some data types. `newCachedThreadPool()` is
  the safe choice; if you must bound it, size it at 6 plus headroom.
- **Make order placement idempotent.** Generate the order id (`setOrderId(UUID...)`) *before*
  sending and store it, so a retry or a reconnect cannot submit the same intent twice.

## Step 6 — Enforce resilience

- **Use `ResilienceConfiguration` for unary calls** rather than hand-rolled retry. It supports
  retry, circuit breaker, rate limiter and bulkhead, per method or as defaults. Default retry
  covers `RESOURCE_EXHAUSTED`, `UNAVAILABLE`, and `INTERNAL` code 70001 — retrying other
  statuses is usually wrong (an `INVALID_ARGUMENT` will never succeed on retry).
- **Use the resilient stream wrappers** (`MarketDataStreamWrapper`,
  `ResilienceServerSideStreamWrapper`) — they reconnect and resubscribe on inactivity. Do not
  write your own reconnect loop on top; you will fight it.
- **Respect rate limits.** On `RESOURCE_EXHAUSTED`, read `getRateLimitReset()` and back off
  until then; log `getTrackingId()` so support can trace the request.
- **Always register a shutdown hook.** Close streams, shut down executors, let in-flight orders
  settle. A bot that exits without `shutdown()`/`disconnect()` leaves subscriptions dangling.
- **Sandbox first.** Any new or changed execution path should be run with `sandbox.enabled=true`
  before real money.

## Anti-patterns — flag these when you see them

- Constructing a stub directly (`newBlockingStub(channel)`) instead of via `ServiceStubFactory`.
- `callSyncMethod` invoked from inside a stream listener (blocks the stream thread).
- A plain `HashMap` or non-atomic counter updated from a listener.
- Unbounded queue, or `Thread.sleep` used for pacing in a callback.
- Hand-rolled reconnect logic alongside a resilient wrapper.
- Catching `Exception`/`Throwable` around a trading action and swallowing it — if the order
  state is now unknown, say so and reconcile, do not just `log.warn`.
- Trading on a stream payload before the subscription is confirmed / while connection state is
  unknown.

## Reference files

Load these as needed — each is self-contained:

- `references/core-api.md` — `ConnectorConfiguration`, stub factories, resilience config,
  exceptions, helpers (money/time mappers). Read before writing any connector code.
- `references/streaming.md` — stream wrapper hierarchy, listeners, `MarketDataStreamManager`
  subscribe/unsubscribe, wrapper payload types. Read before any subscription code.
- `references/strategy-and-spring.md` — `StrategyFactory`/`CandleStrategy` and the
  `CandleTradingBot` Spring Boot starter. Read when the task is candle/ta4j based.
- `references/trading-operations.md` — accounts, sandbox, placing/cancelling orders, lot sizing,
  price rounding, storage repositories. Read before any execution code.
- `references/thread-safety-resilience.md` — the deep-dive: layered concurrency model, code
  patterns, failure catalogue. Read when the task mentions reliability, a bug involving
  duplicates or races, or "make it production-ready".
- `assets/BotSkeleton.java` — a minimal, correctly-structured bot to copy and adapt.
