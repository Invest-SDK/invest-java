# Strategy module and Spring Boot starter

Two higher-level entry points. Prefer them when the task is candle/ta4j based; drop to core only
when the requirement falls outside them.

## Strategy module (`java-sdk-strategy`)

`ru.ttech.piapi.strategy.StrategyFactory` builds strategies on top of a `MarketDataStreamManager`:

```java
var manager = StreamManagerFactory.create(streamFactory)
    .newMarketDataStreamManager(executor, scheduledExecutor);
var strategyFactory = StrategyFactory.create(manager);
var strategy = strategyFactory.newCandleStrategy(configuration);
strategy.run();
```

### CandleStrategyConfiguration

Built with `CandleStrategyConfiguration.builder()`:

```java
CandleStrategyConfiguration.builder()
    .setCandleSource(GetCandlesRequest.CandleSource.CANDLE_SOURCE_INCLUDE_WEEKEND)
    .setWarmupLength(100)                       // bars loaded before live data, to warm indicators
    .setStrategies(Map.of(candleInstrument, barSeries -> strategy))  // Function<BarSeries, Strategy>
    .setStrategyEnterAction((instrument, bar) -> ...)      // BiConsumer<CandleInstrument, Bar>
    .setStrategyExitAction((instrument, bar) -> ...)
    .setStrategyEnterActionSeries((instrument, barSeries) -> ...)  // BiConsumer<..., BarSeries>
    .setStrategyExitActionSeries((instrument, barSeries) -> ...)
    .build();
```

- `setStrategies` maps each `CandleInstrument` to a **`Function<BarSeries, Strategy>`** — the
  function is applied once per instrument to build that instrument's ta4j strategy over its own
  bar series. This is why the same lambda can be reused for several instruments without sharing state.
- You can supply the `Bar` callback, the `BarSeries` callback, or both; if only the `Bar` callback is
  given it is invoked with the last bar of the series. `Bar` has `getClosePrice()` (→ a
  `Num`, use `.bigDecimalValue()` for `BigDecimal`).
- `warmupLength` also becomes the bar series' max bar count, so set it to at least your longest
  indicator period. Too small a value is the usual cause of "the indicator is null / strategy never
  fires" at startup.

`strategy.run()` is asynchronous: it downloads warmup history for every instrument, *then* subscribes
to live candles and starts the manager. Signals won't appear until that completes — don't assume the
bot is live the instant `run()` returns.

The strategy invokes the enter/exit callbacks from the market-data consumer thread. Keep the callbacks
non-blocking; hand work to your own executor if it involves I/O.

### Backtest

`BacktestStrategyFactory` + `CandleStrategyBacktest` backtest candle strategies against historical
data (zipped history, aggregated to the target timeframe). `BarsLoader` downloads and stores
historical candles per instrument as CSV (`start_time,open,high,low,close,volume`). See the module
README and `example/basic-example/.../strategy/backtest/` for the runnable shapes. Use the backtest
to pick parameters, then run the same `Strategy` factory function live — the two share the
`Function<BarSeries, Strategy>` shape deliberately.

## Spring Boot starter (`java-sdk-spring-boot-starter`)

Auto-configures the whole chain as beans: `ConnectorConfiguration`, `ServiceStubFactory`,
`StreamServiceStubFactory`, `StreamManagerFactory`, and a `MarketDataStreamManager` (backed by a
cached-thread-pool executor and a single-threaded scheduled pool). You supply a `ServiceStubFactory`
bean yourself only if you want to override; otherwise just set the token:

```yaml
invest:
  connector:
    token: ${INVEST_TOKEN}
```

To make a bot, implement `CandleTradingBot` (`ru.ttech.piapi.springboot.bot`) as a `@Component`:

```java
public interface CandleTradingBot {
  GetCandlesRequest.CandleSource getCandleSource();
  int getWarmupLength();
  Map<CandleInstrument, Function<BarSeries, Strategy>> getStrategies();
  void onStrategyEnterAction(CandleInstrument instrument, Bar bar);
  void onStrategyExitAction(CandleInstrument instrument, Bar bar);
  default void onStrategyEnterAction(CandleInstrument instrument, BarSeries barSeries) {}
  default void onStrategyExitAction(CandleInstrument instrument, BarSeries barSeries) {}
}
```

The starter's `TradingBotInitializer` (`InitializingBean`) wires all `CandleTradingBot` beans to the
`StrategyFactory` at application start, calling `run()` on a background executor. Inject
`ConnectorConfiguration` and `ServiceStubFactory` into the bot's constructor to reach unary services
for execution:

```java
@Component
public class MyBot implements CandleTradingBot {
  private final SyncStubWrapper<OrdersServiceGrpc.OrdersServiceBlockingStub> orders;
  public MyBot(ConnectorConfiguration cfg, ServiceStubFactory stubs) {
    this.orders = stubs.newSyncService(OrdersServiceGrpc::newBlockingStub);
  }
  // ... getCandleSource / getWarmupLength / getStrategies / onStrategyEnterAction / ...
}
```

Sandbox is honoured via `invest.connector.sandbox.enabled` — the auto-configured
`ConnectorConfiguration` reads it, and `getTargetUrl()` returns the sandbox endpoint accordingly.
Run new bots in the sandbox first.

### Which to choose

- Spring app already, or you want warmup + subscription + dispatch handled → **starter**.
- Candle/ta4j bot outside Spring → **strategy module**.
- Anything else (order book, trades-only, custom execution, multiple accounts) → **core** (see
  `core-api.md` and `streaming.md`).
