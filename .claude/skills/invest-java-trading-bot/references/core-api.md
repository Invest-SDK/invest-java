# Core API — connector, stubs, resilience, exceptions

Module: `java-sdk-core`. Package root `ru.ttech.piapi.core.connector`. Generated protobuf types live
in `ru.tinkoff.piapi.contract.v1`.

## ConnectorConfiguration

Immutable (all fields `private final`, Lombok `@Getter`), so a single instance can be shared across
threads freely. **No builder and no public constructor** — three static factories only:

```java
ConnectorConfiguration.loadFromProperties(Properties properties)        // throws if token missing
ConnectorConfiguration.loadPropertiesFromResources(String filename)     // classpath
ConnectorConfiguration.loadPropertiesFromFile(String filename)          // filesystem
```

> Naming trap: it is `loadPropertiesFromResources` / `loadPropertiesFromFile` (no `From` after
> `load`), but `loadFromProperties`. The module READMEs still show an older name,
> `loadFromPropertiesFile`, which does not exist in the code — trust this file and the source, not
> the README examples.

Property keys and defaults:

| Key | Default | Notes |
|---|---|---|
| `token` | — | required; `IllegalArgumentException` if absent |
| `app.name` | `tinkoff.invest-api-java-sdk` | sent as `x-app-name` |
| `target` | `invest-public-api.tbank.ru:443` | live endpoint |
| `sandbox.target` | `sandbox-invest-public-api.tbank.ru:443` | sandbox endpoint |
| `sandbox.enabled` | `false` | switches `getTargetUrl()` |
| `connection.timeout` | `30000` | CONNECT_TIMEOUT, ms |
| `connection.keepalive` | `60000` | keepAliveTimeout, ms |
| `connection.retry.max-attempts` | `3` | default retry |
| `connection.retry.wait-duration` | `2000` | ms between retries |
| `connection.max-message-size` | `16777216` | bytes |
| `connection.ssl.insecure` | `false` | disable cert verification |
| `connection.ssl.secure-internal` | `true` | uses bundled Russian trusted CA JKS |
| `grpc.debug` | `false` | attaches logging interceptor |
| `grpc.context-fork` | `false` | |
| `stream.market-data.max-streams-count` | `16` | used by `MarketDataStreamManager` |
| `stream.market-data.max-subscriptions-count` | `300` | per stream |
| `stream.market-data.max-resubscription-attempts` | `3` | per-instrument resubscribe budget |
| `stream.inactivity-timeout` | `15000` | ms; triggers reconnect |
| `stream.ping-delay` | `5000` | ms; health-check cadence |

Useful getters: `getToken()`, `getAppName()`, `getTargetUrl()`, `isSandboxEnabled()`, `getTimeout()`,
`getKeepalive()`, `isGrpcDebug()`, `getMaxMarketDataStreamsCount()`,
`getMaxMarketDataSubscriptionsCount()`, `getStreamInactivityTimeout()`, `getStreamPingDelay()`.

## ServiceStubFactory

```java
ServiceStubFactory.create(ConnectorConfiguration configuration)   // only public entry point

<S extends AbstractBlockingStub<S>> SyncStubWrapper<S>
    newSyncService(Function<Channel, S> stubConstructor)
<S extends AbstractAsyncStub<S>> AsyncStubWrapper<S>
    newAsyncService(Function<Channel, S> stubConstructor)
<S extends AbstractBlockingStub<S>> ResilienceSyncStubWrapper<S>
    newResilienceSyncService(Function<Channel, S> stubConstructor, ResilienceConfiguration cfg)
<S extends AbstractAsyncStub<S>> ResilienceAsyncStubWrapper<S>
    newResilienceAsyncService(Function<Channel, S> stubConstructor, ResilienceConfiguration cfg)

ManagedChannel getChannel()
String getTargetUrl()                    // sandbox target when sandbox.enabled
ConnectorConfiguration getConfiguration()
```

**There are no typed stub getters** — no `getInstruments()`, `getOrders()`, etc. You pass the
generated stub constructor yourself:

```java
var orders      = stubs.newSyncService(OrdersServiceGrpc::newBlockingStub);
var instruments = stubs.newSyncService(InstrumentsServiceGrpc::newBlockingStub);
var sandbox     = stubs.newSyncService(SandboxServiceGrpc::newBlockingStub);
var operations  = stubs.newSyncService(OperationsServiceGrpc::newBlockingStub);
var users       = stubs.newSyncService(UsersServiceGrpc::newBlockingStub);
var marketDataAsync = stubs.newAsyncService(MarketDataServiceGrpc::newStub);
```

The factory holds **one shared `ManagedChannel`** (created lazily, thread-safe) and attaches the
`Authorization: Bearer <token>` / `x-app-name` metadata plus the error-reporting interceptor to
every stub. **Never construct a gRPC stub directly** — you would bypass auth and interceptors, and
you would create a second channel.

`SyncStubWrapper.callSyncMethod(Function<S, T>)` runs the call and wraps any `Throwable` in
`ServiceRuntimeException`. `AsyncStubWrapper.callAsyncMethod(BiConsumer<S, StreamObserver<T>>)`
returns `CompletableFuture<T>` that completes exceptionally with `ServiceRuntimeException`.

Resilience variants take `MethodDescriptor` as the first argument:
```java
resilienceSync.callSyncMethod(InstrumentsServiceGrpc.getSharesMethod(), stub -> stub.shares(req));
```

## ResilienceConfiguration

Built via builder; supports retry, circuit breaker, rate limiter and bulkhead, per-method or default:

```java
var executor = Executors.newSingleThreadScheduledExecutor();
var resilience = ResilienceConfiguration.builder(executor, configuration)
    .withDefaultRetry(RetryConfig.custom().waitDuration(Duration.ofMillis(3000)).maxAttempts(5).build())
    .withRetryForMethod(MarketDataServiceGrpc.getGetCandlesMethod(), otherRetryConfig)
    .withDefaultCircuitBreaker(...)   .withCircuitBreakerForMethod(method, ...)
    .withDefaultRateLimiter(...)      .withRateLimiterForMethod(method, ...)
    .withDefaultBulkHead(...)         .withBulkheadForMethod(method, ...)   // note capital "H" in BulkHead
    .build();
```

- Per-method keys are the gRPC full method name; the default key is `"default"`.
- `build()` auto-fills a default for each pattern you didn't set.
- Default retry only retries `RESOURCE_EXHAUSTED`, `UNAVAILABLE`, and `INTERNAL` with code `70001`.
  On `RESOURCE_EXHAUSTED` it honours the `x-ratelimit-reset` header.
- Decorator order applied: bulkhead → rate limiter → circuit breaker → retry.
- Build it on one thread before sharing; the builder uses plain maps. The resulting instances are
  thread-safe.
- The `ScheduledExecutorService` you pass is retained — shut it down when the bot stops.

## StreamServiceStubFactory

```java
StreamServiceStubFactory.create(ServiceStubFactory serviceStubFactory)

<S,RespT> ServerSideStreamWrapper<S, RespT>
    newServerSideStream(ServerSideStreamConfiguration<S,ReqT,RespT> configuration)
<ReqT,RespT> ResilienceServerSideStreamWrapper<ReqT, RespT>
    newResilienceServerSideStream(ResilienceServerSideStreamWrapperConfiguration<ReqT,RespT> cfg)
<S,ReqT,RespT> BidirectionalStreamWrapper<S,ReqT,RespT>
    newBidirectionalStream(BidirectionalStreamConfiguration<S,ReqT,RespT> configuration)
MarketDataStreamWrapper newResilienceMarketDataStream(MarketDataStreamWrapperConfiguration cfg)
ServiceStubFactory getServiceStubFactory()
```

Composition, not inheritance: it reuses the unary factory's shared channel. Only `create` is public.

## Exceptions

`ServiceRuntimeException extends RuntimeException` is the one class callers catch:

```java
try {
  orders.callSyncMethod(stub -> stub.postOrder(request));
} catch (ServiceRuntimeException e) {
  switch (e.getErrorType()) {
    case RESOURCE_EXHAUSTED -> backoffUntil(e.getRateLimitReset());
    case INVALID_ARGUMENT  -> failFast(e.getDescription());
    default                -> { log.error("trackingId={}", e.getTrackingId(), e); throw e; }
  }
}
```

Members: `getErrorStatus()`, `getErrorType()` (`Status.Code`), `getErrorCode()`, `parseErrorCode()`,
`getRateLimitReset()`, `getDescription()`, `getTrackingId()`, `getMetadata()`, `getThrowable()`.
Always log `getTrackingId()` — it is what T-Invest support uses to trace a request.

Also possible: `ConnectorConfiguration.LoadPropertiesError` when config loading fails.

## Helpers (`ru.ttech.piapi.core.helpers`)

protobuf has no float/double and its own timestamp, so conversions are needed constantly. These are
null-safe and used by all the typed wrappers:

```java
NumberMapper.quotationToBigDecimal(Quotation)      // {units, nanos} -> BigDecimal
NumberMapper.moneyValueToBigDecimal(MoneyValue)    // includes currency
NumberMapper.bigDecimalToQuotation(BigDecimal)     // -> Quotation, for order prices
NumberMapper.bigDecimalToMoneyValue(BigDecimal, String currency)

TimeMapper.timestampToLocalDateTime(Timestamp)     // UTC
TimeMapper.localDateTimeToTimestamp(LocalDateTime)
```

Use these rather than hand-rolling units/nanos arithmetic. `FuturePriceMapper` handles futures
price/step. Money and quantities stay `BigDecimal` — never route prices through `double`.
