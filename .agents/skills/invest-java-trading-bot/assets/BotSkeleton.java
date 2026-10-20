package com.example.bot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.piapi.contract.v1.CandleInstrument;
import ru.tinkoff.piapi.contract.v1.GetCandlesRequest;
import ru.tinkoff.piapi.contract.v1.InstrumentsServiceGrpc;
import ru.tinkoff.piapi.contract.v1.OrderDirection;
import ru.tinkoff.piapi.contract.v1.OrderType;
import ru.tinkoff.piapi.contract.v1.PostOrderAsyncRequest;
import ru.tinkoff.piapi.contract.v1.OrdersServiceGrpc;
import ru.tinkoff.piapi.contract.v1.SubscriptionInterval;
import ru.ttech.piapi.core.connector.ConnectorConfiguration;
import ru.ttech.piapi.core.connector.ServiceStubFactory;
import ru.ttech.piapi.core.connector.SyncStubWrapper;
import ru.ttech.piapi.core.connector.streaming.StreamManagerFactory;
import ru.ttech.piapi.core.connector.streaming.StreamServiceStubFactory;
import ru.ttech.piapi.core.helpers.NumberMapper;
import ru.ttech.piapi.core.impl.marketdata.MarketDataStreamManager;
import ru.ttech.piapi.core.impl.marketdata.subscription.CandleSubscriptionSpec;
import ru.ttech.piapi.core.impl.marketdata.subscription.Instrument;
import ru.ttech.piapi.core.impl.marketdata.wrapper.CandleWrapper;

import java.math.BigDecimal;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Minimal, correctly-structured trading bot.
 *
 * <p>Shows the layering the skill recommends: the stream callback only enqueues; a single decision
 * thread handles ordering and trading; execution is idempotent and isolated behind one method.
 * Adapt the TODO sections to the real strategy and add position tracking / reconnect reconciliation
 * from {@code references/thread-safety-resilience.md}.
 *
 * <p>This is a skeleton, not a complete bot — it has no fill reconciliation and no risk limits.
 */
public class BotSkeleton {

  private static final Logger log = LoggerFactory.getLogger(BotSkeleton.class);

  // --- configuration & connection (all immutable, safe to share) ---------------------------
  private final ConnectorConfiguration configuration;
  private final ServiceStubFactory stubs;
  private final MarketDataStreamManager marketData;
  private final SyncStubWrapper<OrdersServiceGrpc.OrdersServiceBlockingStub> orders;
  @SuppressWarnings("unused")
  private final SyncStubWrapper<InstrumentsServiceGrpc.InstrumentsServiceBlockingStub> instruments;

  // --- executors, separated by purpose -----------------------------------------------------
  private final ExecutorService streamingPool = Executors.newCachedThreadPool();
  private final ScheduledExecutorService healthPool = Executors.newSingleThreadScheduledExecutor();
  private final ExecutorService decisionPool = Executors.newSingleThreadExecutor();

  // --- bounded hand-off; the listener never blocks -----------------------------------------
  private final BlockingQueue<CandleWrapper> candleQueue = new ArrayBlockingQueue<>(10_000);
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final ConcurrentHashMap<String, String> pendingOrderIds = new ConcurrentHashMap<>();

  private final Set<String> instrumentUids = Set.of(
      "87db07bc-0e02-4e29-90bb-05e8ef791d7b");   // TODO: real UIDs, resolved from tickers at startup
  private String accountId;

  public BotSkeleton(ConnectorConfiguration configuration) {
    this.configuration = configuration;
    this.stubs = ServiceStubFactory.create(configuration);
    this.orders = stubs.newSyncService(OrdersServiceGrpc::newBlockingStub);
    this.instruments = stubs.newSyncService(InstrumentsServiceGrpc::newBlockingStub);

    var streamStubs = StreamServiceStubFactory.create(stubs);
    this.marketData = StreamManagerFactory.create(streamStubs)
        .newMarketDataStreamManager(streamingPool, healthPool);
  }

  public void start() {
    if (!running.compareAndSet(false, true)) {
      return;   // idempotent start
    }
    accountId = resolveAccountId();                       // TODO: implement via UsersServiceGrpc
    Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "bot-shutdown"));

    subscribeMarketData();
    decisionPool.submit(this::decisionLoop);
    marketData.start();                                    // nothing is delivered until this call
    log.info("bot started on {} (sandbox={})", stubs.getTargetUrl(), configuration.isSandboxEnabled());
  }

  public void stop() {
    if (!running.compareAndSet(true, false)) {
      return;
    }
    marketData.shutdown();
    shutdown(decisionPool, streamingPool, healthPool);
    log.info("bot stopped");
  }

  // --- ingestion: enqueue only, never block, never trade -----------------------------------

  private void subscribeMarketData() {
    var instrumentsToWatch = instrumentUids.stream()
        .map(uid -> new Instrument(uid, SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE))
        .collect(java.util.stream.Collectors.toSet());

    marketData.subscribeCandles(
        instrumentsToWatch,
        new CandleSubscriptionSpec(GetCandlesRequest.CandleSource.CANDLE_SOURCE_INCLUDE_WEEKEND),
        candle -> {
          // A listener must never throw: an escaped exception silently kills the SDK's consumer loop.
          try {
            if (!candle.isHistory() && !candleQueue.offer(candle)) {
              log.warn("candle queue full, dropped candle for {}", candle.getInstrumentUid());
            }
          } catch (Throwable t) {
            log.error("candle listener failure", t);
          }
        });
    // TODO: also subscribe to the order-state stream and reconcile positions on (re)connect
  }

  // --- decision: single thread => ordered, race-free trading decisions ----------------------

  private void decisionLoop() {
    while (running.get()) {
      try {
        var candle = candleQueue.poll(200, TimeUnit.MILLISECONDS);
        if (candle == null) {
          continue;
        }
        onBarClosed(candle);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (RuntimeException e) {
        // one bad decision must not kill the loop; state may be unknown, so log loudly
        log.error("decision error, position may need reconciliation", e);
      }
    }
  }

  private void onBarClosed(CandleWrapper candle) {
    // TODO: feed a pure strategy engine here and act on its signal.
    // Kept deliberately trivial so the skeleton focuses on structure, not alpha.
    var signal = Signal.FLAT;
    switch (signal) {
      case ENTER -> placeOrder(candle.getInstrumentUid(), OrderDirection.ORDER_DIRECTION_BUY);
      case EXIT -> placeOrder(candle.getInstrumentUid(), OrderDirection.ORDER_DIRECTION_SELL);
      case FLAT -> { /* nothing */ }
    }
  }

  enum Signal { ENTER, EXIT, FLAT }

  // --- execution: one choke point, idempotent ids ------------------------------------------

  private void placeOrder(String instrumentId, OrderDirection direction) {
    // Generate the id ONCE, before the call that may be retried. Never mint a new id on retry.
    var orderId = UUID.randomUUID().toString();
    pendingOrderIds.put(orderId, instrumentId + ":" + direction);
    var request = PostOrderAsyncRequest.newBuilder()
        .setOrderId(orderId)
        .setAccountId(accountId)
        .setInstrumentId(instrumentId)
        .setDirection(direction)
        .setQuantity(1)                                    // TODO: size from getMaxLots + risk limits
        .setPrice(NumberMapper.bigDecimalToQuotation(BigDecimal.ONE))   // TODO: real price + rounding
        .setOrderType(OrderType.ORDER_TYPE_LIMIT)
        .build();
    try {
      var ack = orders.callSyncMethod(stub -> stub.postOrderAsync(request));
      log.info("order {} accepted: {}", orderId, ack.getOrderRequestId());
    } catch (ru.ttech.piapi.core.connector.exception.ServiceRuntimeException e) {
      log.error("order {} failed type={} trackingId={}", orderId, e.getErrorType(), e.getTrackingId(), e);
      // Do NOT resend with a new id. Reconcile this id against the order-state stream.
    }
  }

  private String resolveAccountId() {
    throw new UnsupportedOperationException("TODO: call UsersServiceGrpc.getAccounts and pick an open account");
  }

  private static void shutdown(ExecutorService... pools) {
    for (var pool : pools) {
      pool.shutdown();
    }
    for (var pool : pools) {
      try {
        if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
          pool.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        pool.shutdownNow();
      }
    }
  }

  public static void main(String[] args) {
    var configuration = ConnectorConfiguration.loadPropertiesFromResources("invest.properties");
    new BotSkeleton(configuration).start();
  }
}
