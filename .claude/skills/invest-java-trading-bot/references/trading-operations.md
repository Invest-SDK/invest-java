# Trading operations — accounts, orders, sandbox, storage

Modules: `core` (unary services) and `storage/*` (market-data persistence).

## Accounts

```java
var users = stubs.newSyncService(UsersServiceGrpc::newBlockingStub);
var accounts = users.callSyncMethod(stub -> stub.getAccounts(
    GetAccountsRequest.newBuilder().setStatus(AccountStatus.ACCOUNT_STATUS_OPEN).build()));
var accountId = accounts.getAccountsList().stream()
    .filter(a -> a.getType() == AccountType.ACCOUNT_TYPE_TINKOFF)
    .findFirst().map(Account::getId)
    .orElseThrow(() -> new IllegalStateException("Нет открытого брокерского счёта"));
```

Resolve the account id once at startup and keep it in a `final` field. In the sandbox, open an
account if none exists (`SandboxServiceGrpc`: `openSandboxAccount(OpenSandboxAccountRequest)`).

## Sandbox

When `sandbox.enabled=true`, `ServiceStubFactory.getTargetUrl()` points at the sandbox endpoint —
all services follow it automatically. Fund the sandbox account so lot checks don't fail:

```java
var sandbox = stubs.newSyncService(SandboxServiceGrpc::newBlockingStub);
var limits = sandbox.callSyncMethod(stub -> stub.getSandboxWithdrawLimits(
    WithdrawLimitsRequest.newBuilder().setAccountId(accountId).build()));
var rub = limits.getMoneyList().stream()
    .filter(m -> m.getCurrency().equals("rub")).findFirst()
    .map(NumberMapper::moneyValueToBigDecimal).orElse(BigDecimal.ZERO);
if (rub.compareTo(target) < 0) {
  sandbox.callSyncMethod(stub -> stub.sandboxPayIn(SandboxPayInRequest.newBuilder()
      .setAccountId(accountId)
      .setAmount(NumberMapper.bigDecimalToMoneyValue(target.subtract(rub), "rub"))
      .build()));
}
```

Any new or changed execution path should be exercised in the sandbox before real money.

## Placing orders

`OrdersServiceGrpc` — `postOrder` (sync/prebuilt result) or `postOrderAsync` (accept-with-request-id).
The async form plus an order-state stream is the robust pattern, because it gives you an idempotent
handle you can reconcile on:

```java
var request = PostOrderAsyncRequest.newBuilder()
    .setOrderId(UUID.randomUUID().toString())        // generate BEFORE sending; store it
    .setAccountId(accountId)
    .setInstrumentId(instrumentId)
    .setDirection(OrderDirection.ORDER_DIRECTION_BUY)
    .setQuantity(lots)
    .setPrice(NumberMapper.bigDecimalToQuotation(price))   // Quotation, not double
    .setOrderType(OrderType.ORDER_TYPE_LIMIT)
    .build();
var ack = orders.callSyncMethod(stub -> stub.postOrderAsync(request));
instrumentLastOrderIds.put(instrumentId, ack.getOrderRequestId());
```

- **`orderId` is your idempotency key.** Generate it before the call and remember it; a retry or a
  reconnect must reuse the same id, never mint a new one — otherwise a dropped response turns into a
  duplicate order. This is the single most important execution-side rule. Per `orders.proto` it must
  be in UID format and at most 36 characters (a `UUID.toString()` fits; so does a deterministic
  `UUID.nameUUIDFromBytes(...)` derived from instrument + candle time + direction, which makes a
  restart on the same candle idempotent too). The same value comes back as `order_request_id` in
  `OrderState` and in the order-state stream, so match fills on that field — not on the exchange
  `order_id`, which is a different identifier. Confirm the echo once in the sandbox before relying on it.
- **Prices go through `NumberMapper.bigDecimalToQuotation`.** Rounding is still on you (see below).
- Limit orders require a price aligned to the instrument's `minPriceIncrement`.

### Lot sizing

Never exceed what the account can fill — ask the API:

```java
var resp = orders.callSyncMethod(stub -> stub.getMaxLots(GetMaxLotsRequest.newBuilder()
    .setAccountId(accountId).setInstrumentId(instrumentId)
    .setPrice(NumberMapper.bigDecimalToQuotation(price)).build()));
long maxBuy  = resp.getBuyLimits().getBuyMaxLots();
long maxSell = resp.getSellLimits().getSellMaxLots();
long quantity = Math.min(desiredLots, maxBuy);
if (quantity < desiredLots) { /* reduce size or skip — do not send an oversized order */ }
```

Market orders exist (`OrderType.ORDER_TYPE_MARKET`) but are rejected for many instruments, especially
outside trading hours. Check `TradingStatusWrapper.isMarketOrderAvailable()` before choosing one.

### Price rounding

Prices must land on the instrument's price step. Get the step and round the *safe* way per direction
— down for buys, up for sells, so a round never crosses into a worse fill than intended:

```java
var minIncrement = NumberMapper.quotationToBigDecimal(
    instruments.callSyncMethod(stub -> stub.getInstrumentBy(InstrumentRequest.newBuilder()
        .setIdType(InstrumentIdType.INSTRUMENT_ID_TYPE_UID).setId(instrumentId).build()))
        .getInstrument().getMinPriceIncrement());

BigDecimal roundDown(BigDecimal price, BigDecimal step) {
  return price.divide(step, 0, RoundingMode.DOWN).multiply(step);
}
BigDecimal roundUp(BigDecimal price, BigDecimal step) {
  return price.divide(step, 0, RoundingMode.UP).multiply(step);
}
```

### Cancelling

```java
var orders = ordersService.callSyncMethod(stub -> stub.getOrders(
    GetOrdersRequest.newBuilder().setAccountId(accountId).build()));
orders.getOrdersList().stream()
    .filter(o -> o.getInstrumentUid().equals(instrumentId))
    .forEach(o -> ordersService.callSyncMethod(stub -> stub.cancelOrder(
        CancelOrderRequest.newBuilder()
            .setAccountId(accountId)
            .setOrderIdType(OrderIdType.ORDER_ID_TYPE_EXCHANGE)
            .setOrderId(o.getOrderId())
            .build())));
```

Note `cancelOrder` takes the **exchange** order id (`OrderIdType.ORDER_ID_TYPE_EXCHANGE`), which is
what `getOrders` returns — not the request id you generated. Keep both distinct.

## Order-state stream

For fill confirmation and PnL, subscribe to the order-state stream rather than polling. It is
server-side, so use the resilient wrapper:

```java
var streamFactory = StreamServiceStubFactory.create(stubs);
var wrapper = streamFactory.newResilienceServerSideStream(
    OrderStateStreamWrapperConfiguration.builder(healthCheckExecutor)
        .addOnResponseListener(resp -> {
          if (resp.hasOrderState()) { var order = resp.getOrderState(); /* ... */ }
        })
        .addOnConnectListener(() -> initPositions())   // fires on every (re)connect
        .build());
wrapper.subscribe(OrderStateStreamRequest.newBuilder().addAccounts(accountId).build());
```

The response is a union — always guard with `hasOrderState()` before reading. `addOnConnectListener`
fires on every reconnect: use it to reconcile positions from `getPortfolio`, because stream events
are **not** replayed for the disconnected window. That gap is the classic source of position drift.

Related server-side streams, all via the same pattern: `TradeStreamWrapperConfiguration` (own
trades), `PortfolioStreamWrapperConfiguration`, `PositionsStreamWrapperConfiguration`.

## Portfolio and PnL

```java
var operations = stubs.newSyncService(OperationsServiceGrpc::newBlockingStub);
var portfolio = operations.callSyncMethod(stub -> stub.getPortfolio(
    PortfolioRequest.newBuilder().setAccountId(accountId)
        .setCurrency(PortfolioRequest.CurrencyRequest.RUB).build()));
portfolio.getPositionsList() // each: getInstrumentUid(), getAveragePositionPrice() (MoneyValue)
```

Reconcile from here on every reconnect. Track realised PnL yourself from fill events — the API gives
current positions, not a running realised-PnL figure.

## Storage (`java-sdk-storage-*`)

Repositories implement `ReadWriteRepository<T>` (`save`, `saveBatch`, `findAll`,
`findAllByTimeAndInstrumentUid`, `findAllByPeriodAndInstrumentUid`) for `Candle`, `LastPrice`,
`Trade`, `TradingStatus`, `OrderBook`.

- **CSV** (`java-sdk-storage-csv`): `new CandleSubscriptionSpec()`-style wiring;
  `var repo = new CandlesCsvRepository(new CsvConfiguration(Path.of("candles.csv")))`. It is
  `Closeable` — use try-with-resources. CSV repos are **not** meant for concurrent writers.
- **JDBC** (`java-sdk-storage-jdbc`): `new CandlesJdbcRepository(new JdbcConfiguration(dataSource,
  "schema", "table"))`. Postgres and MySQL supported; add the JDBC driver yourself.

Persist from the market-data consumer thread is a common mistake — a slow DB write there blocks the
consumer and stalls every subscription. Enqueue and let a separate writer thread persist:

```java
manager.subscribeCandles(instruments, new CandleSubscriptionSpec(),
    candle -> persistQueue.offer(candle.getOriginal()));   // bounded ArrayBlockingQueue
// dedicated writer thread: candlesRepository.saveBatch(drain(persistQueue));
```

Repository implementations themselves are not guaranteed thread-safe for concurrent writers — one
writer thread per repository is the safe arrangement.

## Instrument lookup

`InstrumentsServiceGrpc` — `shares`/`bonds`/`etfs`/`futures`/`currencies`/`options` list by type;
`getInstrumentBy` resolves one. Filter tradability before subscribing:

```java
share.getApiTradeAvailableFlag()
    && share.getTradingStatus() == SecurityTradingStatus.SECURITY_TRADING_STATUS_NORMAL_TRADING
```

Resolve ticker → UID once at startup: the same ticker can map to several instruments around corporate
actions, and UIDs are what the streams and orders use. See `example/.../cmd/client/instruments/`.
