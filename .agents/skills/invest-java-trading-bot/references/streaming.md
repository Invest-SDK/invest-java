# Streaming & market data

Package: `ru.ttech.piapi.core.connector.streaming`, `ru.ttech.piapi.core.impl.marketdata`.

## Two stream kinds

- **Server-side** — the subscription is fixed at connect time. To change it you reconnect. Used for
  portfolio, positions, order state and trades streams.
- **Bidirectional** — you can change the subscription on a live connection by sending a new request.
  Used for market data (candles, last price, order book, trades, trading status).

## Wrapper hierarchy

Low-level wrappers are created **only** via `StreamServiceStubFactory` (constructors are
package-private):

| Wrapper | Methods |
|---|---|
| `ServerSideStreamWrapper<S, RespT>` | `connect()`, `disconnect()` |
| `BidirectionalStreamWrapper<S, ReqT, RespT>` | `connect()`, `disconnect()`, `newCall(ReqT)`, `isConnected()` |
| `ResilienceServerSideStreamWrapper<ReqT, RespT>` | `subscribe(ReqT)` (throws if already subscribed), `disconnect()` |
| `MarketDataStreamWrapper` | `subscribe(MarketDataRequest)`, `disconnect()` |

The resilience wrappers are the ones you normally want. They run a health-check on a fixed schedule
(`stream.ping-delay`) and, when no activity has been seen for `stream.inactivity-timeout`, disconnect
and re-send the last successful subscription request. `ResilienceServerSideStreamWrapper` and
`MarketDataStreamWrapper` are created with a `*WrapperConfiguration` built on a
`ScheduledExecutorService`. Both only reconnect and re-send the subscription; they do **not** backfill
data lost during the gap.

**Missed-candle recovery lives in `MarketDataManager`, not in the stream wrapper or the stream
manager.** `new MarketDataManager(streamManager)` wraps a `MarketDataStreamManager`, exposes the same
`subscribeCandles`/`start()`/`shutdown()` surface, and after a reconnect backfills the gap through
`GetCandles`, replaying the missing candles into your listener with `isHistory() == true`.
`CandleStrategy` (strategy module, and therefore the Spring starter) already uses it. A hand-wired bot
that subscribes straight on `MarketDataStreamManager` gets **no** backfill — wrap it in
`MarketDataManager` if the strategy cannot tolerate gaps (indicators over a series need contiguous
bars). `MarketDataManager.shutdown()` also shuts down its own internal executor.

The inactivity timeout and ping delay come from `ConnectorConfiguration` — they are **not**
per-wrapper builder settings. To tune them, set `stream.inactivity-timeout` / `stream.ping-delay` in
the properties file.

## Registering listeners

Listener interfaces (`connector/streaming/listeners`):

```java
OnNextListener<RespT>   → void onNext(RespT response);
OnErrorListener         → void onError(Throwable throwable);
OnCompleteListener      → void onComplete();
OnConnectListener<T>    → void onConnect(T input);
```

The method is `onNext`, not `onResponse`. On `MarketDataStreamConfiguration.Builder`
(`MarketDataStreamConfiguration.builder()`):

```java
addOnCandleListener(OnNextListener<CandleWrapper>)
addOnLastPriceListener(OnNextListener<LastPriceWrapper>)
addOnOrderBookListener(OnNextListener<OrderBookWrapper>)
addOnTradeListener(OnNextListener<TradeWrapper>)
addOnOpenInterestListener(OnNextListener<OpenInterestWrapper>)
addOnTradingStatusListener(OnNextListener<TradingStatusWrapper>)
addOnNextListener(OnNextListener<MarketDataResponse>)   // raw, catch-all
addOnErrorListener(OnErrorListener)
addOnCompleteListener(OnCompleteListener)
```

`MarketDataStreamWrapperConfiguration.builder(scheduledExecutor)` has the same `addOn*` listeners
plus `addOnConnectListener(OnConnectListener<OnConnectEvent>)`. This hook is available only when you
build a `MarketDataStreamWrapper` yourself; `MarketDataStreamManager.addOnConnectListener` is
`protected` (used internally by `MarketDataManager`), so a manager-based bot cannot register one.

The server-side `*WrapperConfiguration` classes (`OrderStateStreamWrapperConfiguration`,
`TradeStreamWrapperConfiguration`, `PortfolioStreamWrapperConfiguration`,
`PositionsStreamWrapperConfiguration`) expose just `addOnResponseListener(OnNextListener<RespT>)` and
`addOnConnectListener(Runnable)`, built with `builder(scheduledExecutor)`.

## Typed payload wrappers

Wrappers convert protobuf's `Quotation`/`Timestamp` into `BigDecimal`/`LocalDateTime`. `getOriginal()`
(on the base `ResponseWrapper<T>`) returns the raw proto when you need a field the wrapper hides.

**CandleWrapper** — `getOpen()/getHigh()/getLow()/getClose()` → `BigDecimal`; `getVolume()`,
`getVolumeBuy()`, `getVolumeSell()` → `long`; `getTime()`, `getLastTradeTime()` → `LocalDateTime`
(UTC); `getFigi()/getTicker()/getClassCode()/getInstrumentUid()`; `getInterval()`;
`getCandleSourceType()`; `isHistory()`.
> There is **no `isComplete()`** on the wrapper. If you need `Candle.getIsComplete()`, go through
> `getOriginal().getIsComplete()`. `isHistory()` means "backfilled by `MarketDataManager` after a reconnect", not "closed bar". Use history
candles to update indicators, but never to trigger trades — the signal is already in the past.

**LastPriceWrapper** — `getPrice()` BigDecimal; `getTime()`; `getLastPriceType()`; instrument getters.

**OrderBookWrapper** (spelling: capital B) — `getBids()/getAsks()` → `List<OrderWrapper>` (each with
`getPrice()` BigDecimal, `getQuantity()` long); `getDepth()`; `isConsistent()`; `getTime()`;
`getLimitUp()/getLimitDown()`; `getOrderBookType()`.

**TradeWrapper** — `getDirection()` → `TradeDirection`; `getPrice()`; `getQuantity()`;
`getTime()`; `getTradeSource()`.

**TradingStatusWrapper** — `getTradingStatus()` → `SecurityTradingStatus`; `getTime()`;
`isLimitOrderAvailable()`, `isMarketOrderAvailable()`.

**OpenInterestWrapper** — `getOpenInterest()` long; `getTime()`; instrument getters.

## MarketDataStreamManager — the recommended entry point

Distributes subscriptions across multiple underlying streams automatically so you never manage the
per-stream subscription limit (300 by default) yourself.

```java
var manager = StreamManagerFactory.create(streamServiceFactory)
    .newMarketDataStreamManager(executorService, scheduledExecutorService);
```

Subscribe methods:

```java
subscribeCandles(Set<Instrument>, CandleSubscriptionSpec, OnNextListener<CandleWrapper>)
subscribeLastPrices(Set<Instrument>, OnNextListener<LastPriceWrapper>)
subscribeTrades(Set<Instrument>, TradeSourceType, boolean withOpenInterest,
                OnNextListener<TradeWrapper>, OnNextListener<OpenInterestWrapper>)
subscribeOrderBooks(Set<Instrument>, OnNextListener<OrderBookWrapper>)       // plural "OrderBooks"
subscribeTradingStatuses(Set<Instrument>, OnNextListener<TradingStatusWrapper>)
```

Matching `unsubscribe*` methods take the same key arguments (minus listeners). Status checks:
`isSubscribedCandles(Instrument)`, `isSubscribedLastPrice`, `isSubscribedTrades`,
`isSubscribedOrderBook`, `isSubscribedTradingStatuses`. There is a generic
`unsubscribe(MarketDataResponseType, Set<Instrument>, Function<...>)` too.

`start()` launches the consumer loops that drain the internal queues and fan payloads out to
listeners. **Nothing is delivered until you call `start()`** — this is the single most common
"why am I getting no data" cause. Calling `start()` twice spawns duplicate consumers; call it once.
`shutdown()` disconnects every underlying stream — but it does **not** shut down the executor
services you passed in. The caller owns those.

### Instrument

`Instrument` is the subscription key; **use the constructor that matches the data type** — the
constructors differ in which fields they set, and `equals`/`hashCode` cover all of them, so an
instrument built for the wrong data type won't match and the unsubscribe will silently do nothing.

```java
new Instrument(uid)                                          // last price, trades, trading status
new Instrument(uid, SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)   // candles
new Instrument(uid, depth, OrderBookType)                    // order book
new Instrument(ticker, classCode)                            // by ticker+class instead of uid
```

You can pass a UID, a ticker+classCode, or the SDK's `ticker_classCode` form; resolution is handled
internally.

### CandleSubscriptionSpec

```java
new CandleSubscriptionSpec()                       // weekend source, waitingClose = true
new CandleSubscriptionSpec(candleSource)           // waitingClose = true
new CandleSubscriptionSpec(candleSource, waitingClose)
```

`waitingClose = true` delivers only closed candles — what almost every strategy wants, since acting
on a forming candle means your signal can flip before the bar closes.

## Concurrency notes

`MarketDataStreamManager` chains subscribe/unsubscribe calls onto a single future, so calls from
several threads are serialised in practice — but it is not designed for uncoordinated concurrent
subscription. **Drive all `subscribe*`/`unsubscribe*` calls from one thread** (e.g. a single-threaded
executor). The data listeners themselves run on the manager's consumer threads, one loop per data
type; keep each listener short and non-blocking.

**Error and completion hooks exist only on raw streams.** `addOnErrorListener(OnErrorListener)` and
`addOnCompleteListener(OnCompleteListener)` are on `ServerSideStreamConfiguration`,
`BidirectionalStreamConfiguration` and `MarketDataStreamConfiguration` builders. The resilient
builders (`MarketDataStreamWrapperConfiguration`, the `*StreamWrapperConfiguration` family) and
`MarketDataStreamManager` pass **no** error/complete listeners to the underlying stream, and the
stream observer itself logs nothing on `onError`/`onCompleted`. So with resilient wrappers a failed
stream is invisible until the inactivity timeout triggers a reconnect, which logs
`Wrapper {} reconnecting...` (market data) or `Reconnecting...` (server-side) at INFO. Use the raw
hooks to log failures when you build raw streams; with resilient wrappers rely on those INFO lines
and on a freshness watchdog. Listener exceptions are different: for raw streams the observer catches
and logs them (`Произошла ошибка при обработке ответа`); the manager's consumer loop does not.

**Duplicate subscriptions duplicate callbacks.** Each `subscribe*` call wraps the listener in a new
filtering lambda before `addWrapperListener` checks `listeners.contains(...)`. Reference equality
against a new object never matches, so a second call with the same instruments and listener adds a
second copy, and every payload reaches your code twice. Subscribe once per (instrument set, listener),
and keep the strategy callback idempotent anyway.

**Exceptions kill the consumer.** `start()` submits each consumer loop with `executorService.submit`,
and the loop catches only `InterruptedException`. A `RuntimeException` thrown by any listener
terminates that loop; the exception lands in a discarded `Future`, nothing is logged, and because the
underlying gRPC stream is still alive the resilient wrapper's inactivity reconnect does not help.
Always catch `Throwable` inside listener bodies and log it. Listeners of one data type run in a
`forEach`, so one that throws also prevents the listeners registered after it from seeing that item.

Never call a unary service (`callSyncMethod`) from inside a listener — it blocks that consumer thread
and stalls every subscription it feeds.

## Minimal example

```java
var manager = StreamManagerFactory.create(streamFactory)
    .newMarketDataStreamManager(Executors.newCachedThreadPool(),
                                Executors.newSingleThreadScheduledExecutor());

OnNextListener<CandleWrapper> onCandle = candle -> queue.offer(candle);   // bounded queue

manager.subscribeCandles(
    Set.of(new Instrument("87db07bc-0e02-4e29-90bb-05e8ef791d7b",
                          SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)),
    new CandleSubscriptionSpec(),
    onCandle);
manager.start();
```
