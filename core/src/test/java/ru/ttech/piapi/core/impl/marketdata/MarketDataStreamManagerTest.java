package ru.ttech.piapi.core.impl.marketdata;

import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.awaitility.Awaitility;
import org.grpcmock.junit5.GrpcMockExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.tinkoff.piapi.contract.v1.Candle;
import ru.tinkoff.piapi.contract.v1.CandleSubscription;
import ru.tinkoff.piapi.contract.v1.InfoSubscription;
import ru.tinkoff.piapi.contract.v1.MarketDataRequest;
import ru.tinkoff.piapi.contract.v1.MarketDataResponse;
import ru.tinkoff.piapi.contract.v1.MarketDataStreamServiceGrpc;
import ru.tinkoff.piapi.contract.v1.OrderBook;
import ru.tinkoff.piapi.contract.v1.OrderBookSubscription;
import ru.tinkoff.piapi.contract.v1.OrderBookType;
import ru.tinkoff.piapi.contract.v1.SubscribeCandlesResponse;
import ru.tinkoff.piapi.contract.v1.SubscribeInfoResponse;
import ru.tinkoff.piapi.contract.v1.SubscribeOrderBookResponse;
import ru.tinkoff.piapi.contract.v1.SubscribeTradesResponse;
import ru.tinkoff.piapi.contract.v1.SubscriptionAction;
import ru.tinkoff.piapi.contract.v1.SubscriptionInterval;
import ru.tinkoff.piapi.contract.v1.SubscriptionStatus;
import ru.tinkoff.piapi.contract.v1.Trade;
import ru.tinkoff.piapi.contract.v1.TradeSourceType;
import ru.tinkoff.piapi.contract.v1.TradeSubscription;
import ru.tinkoff.piapi.contract.v1.TradingStatus;
import ru.ttech.piapi.core.connector.GrpcStubBaseTest;
import ru.ttech.piapi.core.connector.streaming.StreamManagerFactory;
import ru.ttech.piapi.core.connector.streaming.StreamServiceStubFactory;
import ru.ttech.piapi.core.impl.marketdata.subscription.CandleSubscriptionSpec;
import ru.ttech.piapi.core.impl.marketdata.subscription.Instrument;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.grpcmock.GrpcMock.bidiStreamingMethod;
import static org.grpcmock.GrpcMock.stubFor;

@ExtendWith(GrpcMockExtension.class)
public class MarketDataStreamManagerTest extends GrpcStubBaseTest {

  @SneakyThrows
  @Test
  void testSubscribeTradesDoesNotDuplicateAcrossSeparateCalls() {
    var instrumentA = "instrumentA";
    var instrumentB = "instrumentB";
    var tradesForListenerA = new CopyOnWriteArrayList<String>();
    var tradesForListenerB = new CopyOnWriteArrayList<String>();
    var requestsReceived = new AtomicInteger();

    stubFor(bidiStreamingMethod(MarketDataStreamServiceGrpc.getMarketDataStreamMethod())
      .withFirstRequest(MarketDataRequest::hasSubscribeTradesRequest)
      .willProxyTo(responseObserver -> new StreamObserver<>() {
        @Override
        public void onNext(MarketDataRequest request) {
          if (!request.hasSubscribeTradesRequest()) {
            return;
          }
          var subscribedInstrumentId = request.getSubscribeTradesRequest().getInstruments(0).getInstrumentId();
          responseObserver.onNext(MarketDataResponse.newBuilder()
            .setSubscribeTradesResponse(SubscribeTradesResponse.newBuilder()
              .addTradeSubscriptions(TradeSubscription.newBuilder()
                .setInstrumentUid(subscribedInstrumentId)
                .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
                .setSubscriptionStatus(SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
                .build())
              .build())
            .build());
          if (requestsReceived.incrementAndGet() == 2) {
            try {
              Thread.sleep(50);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            responseObserver.onNext(MarketDataResponse.newBuilder()
              .setTrade(Trade.newBuilder().setInstrumentUid(instrumentA).build())
              .build());
            responseObserver.onNext(MarketDataResponse.newBuilder()
              .setTrade(Trade.newBuilder().setInstrumentUid(instrumentB).build())
              .build());
          }
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      }));

    var factoryWithConfig = createStubFactory();
    var streamFactory = StreamServiceStubFactory.create(factoryWithConfig._1());
    var streamManagerFactory = StreamManagerFactory.create(streamFactory);
    var executorService = Executors.newFixedThreadPool(4);
    var scheduledExecutorService = Executors.newScheduledThreadPool(2);
    var manager = streamManagerFactory.newMarketDataStreamManager(executorService, scheduledExecutorService);

    try {
      manager.subscribeTrades(
        Set.of(new Instrument(instrumentA)),
        TradeSourceType.TRADE_SOURCE_ALL,
        false,
        trade -> tradesForListenerA.add(trade.getInstrumentUid()),
        openInterest -> { }
      );
      manager.subscribeTrades(
        Set.of(new Instrument(instrumentB)),
        TradeSourceType.TRADE_SOURCE_ALL,
        false,
        trade -> tradesForListenerB.add(trade.getInstrumentUid()),
        openInterest -> { }
      );
      manager.start();

      Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> {
          assertThat(tradesForListenerA).containsExactly(instrumentA);
          assertThat(tradesForListenerB).containsExactly(instrumentB);
        });
    } finally {
      manager.shutdown();
      executorService.shutdownNow();
      scheduledExecutorService.shutdownNow();
    }
  }

  @SneakyThrows
  @Test
  void testSubscribeCandlesWithDifferentIntervalsForSameInstrument() {
    var instrumentUid = "instrumentA";
    var ticker = "TICKER";
    var classCode = "CLASS";
    var figi = "figiA";
    var oneMinuteInterval = SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE;
    var fiveMinutesInterval = SubscriptionInterval.SUBSCRIPTION_INTERVAL_FIVE_MINUTES;
    var candlesReceived = new CopyOnWriteArrayList<SubscriptionInterval>();

    stubFor(bidiStreamingMethod(MarketDataStreamServiceGrpc.getMarketDataStreamMethod())
      .withFirstRequest(MarketDataRequest::hasSubscribeCandlesRequest)
      .willProxyTo(responseObserver -> new StreamObserver<>() {
        @Override
        public void onNext(MarketDataRequest request) {
          if (!request.hasSubscribeCandlesRequest()) {
            return;
          }
          responseObserver.onNext(MarketDataResponse.newBuilder()
            .setSubscribeCandlesResponse(SubscribeCandlesResponse.newBuilder()
              .addAllCandlesSubscriptions(request.getSubscribeCandlesRequest().getInstrumentsList().stream()
                .map(candleInstrument -> CandleSubscription.newBuilder()
                  .setInstrumentUid(instrumentUid)
                  .setTicker(ticker)
                  .setClassCode(classCode)
                  .setFigi(figi)
                  .setInterval(candleInstrument.getInterval())
                  .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
                  .setSubscriptionStatus(SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
                  .build())
                .toList())
              .build())
            .build());
          responseObserver.onNext(MarketDataResponse.newBuilder()
            .setCandle(Candle.newBuilder()
              .setInstrumentUid(instrumentUid)
              .setTicker(ticker)
              .setClassCode(classCode)
              .setFigi(figi)
              .setInterval(oneMinuteInterval)
              .build())
            .build());
          responseObserver.onNext(MarketDataResponse.newBuilder()
            .setCandle(Candle.newBuilder()
              .setInstrumentUid(instrumentUid)
              .setTicker(ticker)
              .setClassCode(classCode)
              .setFigi(figi)
              .setInterval(fiveMinutesInterval)
              .build())
            .build());
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      }));

    var factoryWithConfig = createStubFactory();
    var streamFactory = StreamServiceStubFactory.create(factoryWithConfig._1());
    var streamManagerFactory = StreamManagerFactory.create(streamFactory);
    var executorService = Executors.newFixedThreadPool(4);
    var scheduledExecutorService = Executors.newScheduledThreadPool(2);
    var manager = streamManagerFactory.newMarketDataStreamManager(executorService, scheduledExecutorService);

    try {
      manager.subscribeCandles(
        Set.of(
          new Instrument(instrumentUid, oneMinuteInterval),
          new Instrument(instrumentUid, fiveMinutesInterval)
        ),
        new CandleSubscriptionSpec(),
        candle -> candlesReceived.add(candle.getInterval())
      );
      manager.start();

      Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertThat(candlesReceived)
          .containsExactlyInAnyOrder(oneMinuteInterval, fiveMinutesInterval));
    } finally {
      manager.shutdown();
      executorService.shutdownNow();
      scheduledExecutorService.shutdownNow();
    }
  }

  @SneakyThrows
  @Test
  void testSubscribeTradingStatusesDeliversToListener() {
    var instrumentUid = "instrumentA";
    var ticker = "TICKER";
    var classCode = "CLASS";
    var figi = "figiA";
    var statusesReceived = new CopyOnWriteArrayList<String>();

    stubFor(bidiStreamingMethod(MarketDataStreamServiceGrpc.getMarketDataStreamMethod())
      .withFirstRequest(MarketDataRequest::hasSubscribeInfoRequest)
      .willProxyTo(responseObserver -> new StreamObserver<>() {
        @Override
        public void onNext(MarketDataRequest request) {
          if (!request.hasSubscribeInfoRequest()) {
            return;
          }
          responseObserver.onNext(MarketDataResponse.newBuilder()
            .setSubscribeInfoResponse(SubscribeInfoResponse.newBuilder()
              .addInfoSubscriptions(InfoSubscription.newBuilder()
                .setInstrumentUid(instrumentUid)
                .setTicker(ticker)
                .setClassCode(classCode)
                .setFigi(figi)
                .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
                .setSubscriptionStatus(SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
                .build())
              .build())
            .build());
          responseObserver.onNext(MarketDataResponse.newBuilder()
            .setTradingStatus(TradingStatus.newBuilder()
              .setInstrumentUid(instrumentUid)
              .setTicker(ticker)
              .setClassCode(classCode)
              .setFigi(figi)
              .build())
            .build());
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      }));

    var factoryWithConfig = createStubFactory();
    var streamFactory = StreamServiceStubFactory.create(factoryWithConfig._1());
    var streamManagerFactory = StreamManagerFactory.create(streamFactory);
    // start() всегда запускает по одному потребителю на каждый из 6 типов рыночных данных -
    // пула нужно не меньше, иначе часть очередей (например, для торгового статуса) никогда не будет обработана
    var executorService = Executors.newFixedThreadPool(8);
    var scheduledExecutorService = Executors.newScheduledThreadPool(2);
    var manager = streamManagerFactory.newMarketDataStreamManager(executorService, scheduledExecutorService);

    try {
      manager.subscribeTradingStatuses(
        Set.of(new Instrument(instrumentUid)),
        tradingStatus -> statusesReceived.add(tradingStatus.getInstrumentUid())
      );
      manager.start();

      Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertThat(statusesReceived).containsExactly(instrumentUid));
    } finally {
      manager.shutdown();
      executorService.shutdownNow();
      scheduledExecutorService.shutdownNow();
    }
  }

  @SneakyThrows
  @Test
  void testSubscribeTradingStatusesWithSharedIntervalInstrumentStillDelivers() {
    var instrumentUid = "instrumentA";
    var ticker = "TICKER";
    var classCode = "CLASS";
    var figi = "figiA";
    var statusesReceived = new CopyOnWriteArrayList<String>();

    stubFor(bidiStreamingMethod(MarketDataStreamServiceGrpc.getMarketDataStreamMethod())
      .withFirstRequest(MarketDataRequest::hasSubscribeInfoRequest)
      .willProxyTo(responseObserver -> new StreamObserver<>() {
        @Override
        public void onNext(MarketDataRequest request) {
          if (!request.hasSubscribeInfoRequest()) {
            return;
          }
          responseObserver.onNext(MarketDataResponse.newBuilder()
            .setSubscribeInfoResponse(SubscribeInfoResponse.newBuilder()
              .addInfoSubscriptions(InfoSubscription.newBuilder()
                .setInstrumentUid(instrumentUid)
                .setTicker(ticker)
                .setClassCode(classCode)
                .setFigi(figi)
                .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
                .setSubscriptionStatus(SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
                .build())
              .build())
            .build());
          responseObserver.onNext(MarketDataResponse.newBuilder()
            .setTradingStatus(TradingStatus.newBuilder()
              .setInstrumentUid(instrumentUid)
              .setTicker(ticker)
              .setClassCode(classCode)
              .setFigi(figi)
              .build())
            .build());
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      }));

    var factoryWithConfig = createStubFactory();
    var streamFactory = StreamServiceStubFactory.create(factoryWithConfig._1());
    var streamManagerFactory = StreamManagerFactory.create(streamFactory);
    var executorService = Executors.newFixedThreadPool(8);
    var scheduledExecutorService = Executors.newScheduledThreadPool(2);
    var manager = streamManagerFactory.newMarketDataStreamManager(executorService, scheduledExecutorService);

    try {
      // Инструмент создан конструктором для свечей (как если бы он был переиспользован из subscribeCandles) -
      // подписка на статус торгов всё равно должна доставлять обновления листенеру
      manager.subscribeTradingStatuses(
        Set.of(new Instrument(instrumentUid, SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)),
        tradingStatus -> statusesReceived.add(tradingStatus.getInstrumentUid())
      );
      manager.start();

      Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertThat(statusesReceived).containsExactly(instrumentUid));
    } finally {
      manager.shutdown();
      executorService.shutdownNow();
      scheduledExecutorService.shutdownNow();
    }
  }

  @SneakyThrows
  @Test
  void testSubscribeTradingStatusesTwiceRegistersBothListeners() {
    var instrumentUid = "instrumentA";
    var ticker = "TICKER";
    var classCode = "CLASS";
    var figi = "figiA";
    var statusesForListenerA = new CopyOnWriteArrayList<String>();
    var statusesForListenerB = new CopyOnWriteArrayList<String>();

    stubFor(bidiStreamingMethod(MarketDataStreamServiceGrpc.getMarketDataStreamMethod())
      .withFirstRequest(MarketDataRequest::hasSubscribeInfoRequest)
      .willProxyTo(responseObserver -> new StreamObserver<>() {
        @Override
        public void onNext(MarketDataRequest request) {
          if (!request.hasSubscribeInfoRequest()) {
            return;
          }
          responseObserver.onNext(MarketDataResponse.newBuilder()
            .setSubscribeInfoResponse(SubscribeInfoResponse.newBuilder()
              .addInfoSubscriptions(InfoSubscription.newBuilder()
                .setInstrumentUid(instrumentUid)
                .setTicker(ticker)
                .setClassCode(classCode)
                .setFigi(figi)
                .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
                .setSubscriptionStatus(SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
                .build())
              .build())
            .build());
          // Задержка нужна, чтобы тест успел вызвать subscribeTradingStatuses второй раз
          // (с другим листенером) до того, как придёт единственный push
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          responseObserver.onNext(MarketDataResponse.newBuilder()
            .setTradingStatus(TradingStatus.newBuilder()
              .setInstrumentUid(instrumentUid)
              .setTicker(ticker)
              .setClassCode(classCode)
              .setFigi(figi)
              .build())
            .build());
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      }));

    var factoryWithConfig = createStubFactory();
    var streamFactory = StreamServiceStubFactory.create(factoryWithConfig._1());
    var streamManagerFactory = StreamManagerFactory.create(streamFactory);
    var executorService = Executors.newFixedThreadPool(8);
    var scheduledExecutorService = Executors.newScheduledThreadPool(2);
    var manager = streamManagerFactory.newMarketDataStreamManager(executorService, scheduledExecutorService);

    try {
      var instrument = new Instrument(instrumentUid);
      manager.subscribeTradingStatuses(
        Set.of(instrument),
        tradingStatus -> statusesForListenerA.add(tradingStatus.getInstrumentUid())
      );
      manager.start();

      // Дожидаемся, что подписка уже активна, прежде чем повторно подписаться с другим листенером -
      // именно этот сценарий (инструмент уже подписан) приводил к потере второго листенера
      Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(() -> manager.isSubscribedTradingStatuses(instrument));

      manager.subscribeTradingStatuses(
        Set.of(instrument),
        tradingStatus -> statusesForListenerB.add(tradingStatus.getInstrumentUid())
      );

      Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> {
          assertThat(statusesForListenerA).containsExactly(instrumentUid);
          assertThat(statusesForListenerB).containsExactly(instrumentUid);
        });
    } finally {
      manager.shutdown();
      executorService.shutdownNow();
      scheduledExecutorService.shutdownNow();
    }
  }

  @SneakyThrows
  @Test
  void testSubscribeOrderBookWithAllTypeDeliversToListener() {
    var instrumentUid = "instrumentA";
    var ticker = "TICKER";
    var classCode = "CLASS";
    var figi = "figiA";
    var depth = 20;
    var orderBooksReceived = new CopyOnWriteArrayList<String>();

    stubFor(bidiStreamingMethod(MarketDataStreamServiceGrpc.getMarketDataStreamMethod())
      .withFirstRequest(MarketDataRequest::hasSubscribeOrderBookRequest)
      .willProxyTo(responseObserver -> new StreamObserver<>() {
        @Override
        public void onNext(MarketDataRequest request) {
          if (!request.hasSubscribeOrderBookRequest()) {
            return;
          }
          responseObserver.onNext(MarketDataResponse.newBuilder()
            .setSubscribeOrderBookResponse(SubscribeOrderBookResponse.newBuilder()
              .addOrderBookSubscriptions(OrderBookSubscription.newBuilder()
                .setInstrumentUid(instrumentUid)
                .setTicker(ticker)
                .setClassCode(classCode)
                .setFigi(figi)
                .setDepth(depth)
                .setOrderBookType(OrderBookType.ORDERBOOK_TYPE_ALL)
                .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
                .setSubscriptionStatus(SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
                .build())
              .build())
            .build());
          // Реальный пуш по стакану всегда приходит с конкретным типом (биржевой/дилерский),
          // а не ALL, даже если подписка была оформлена как ORDERBOOK_TYPE_ALL
          responseObserver.onNext(MarketDataResponse.newBuilder()
            .setOrderbook(OrderBook.newBuilder()
              .setInstrumentUid(instrumentUid)
              .setTicker(ticker)
              .setClassCode(classCode)
              .setFigi(figi)
              .setDepth(depth)
              .setOrderBookType(OrderBookType.ORDERBOOK_TYPE_EXCHANGE)
              .build())
            .build());
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      }));

    var factoryWithConfig = createStubFactory();
    var streamFactory = StreamServiceStubFactory.create(factoryWithConfig._1());
    var streamManagerFactory = StreamManagerFactory.create(streamFactory);
    var executorService = Executors.newFixedThreadPool(8);
    var scheduledExecutorService = Executors.newScheduledThreadPool(2);
    var manager = streamManagerFactory.newMarketDataStreamManager(executorService, scheduledExecutorService);

    try {
      manager.subscribeOrderBooks(
        Set.of(new Instrument(instrumentUid, depth, OrderBookType.ORDERBOOK_TYPE_ALL)),
        orderBook -> orderBooksReceived.add(orderBook.getInstrumentUid())
      );
      manager.start();

      Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertThat(orderBooksReceived).containsExactly(instrumentUid));
    } finally {
      manager.shutdown();
      executorService.shutdownNow();
      scheduledExecutorService.shutdownNow();
    }
  }

  @SneakyThrows
  @Test
  void testSubscribeOrderBookWithExplicitTypeIgnoresOtherType() {
    var instrumentUid = "instrumentA";
    var ticker = "TICKER";
    var classCode = "CLASS";
    var figi = "figiA";
    var depth = 20;
    var orderBooksReceived = new CopyOnWriteArrayList<String>();

    stubFor(bidiStreamingMethod(MarketDataStreamServiceGrpc.getMarketDataStreamMethod())
      .withFirstRequest(MarketDataRequest::hasSubscribeOrderBookRequest)
      .willProxyTo(responseObserver -> new StreamObserver<>() {
        @Override
        public void onNext(MarketDataRequest request) {
          if (!request.hasSubscribeOrderBookRequest()) {
            return;
          }
          responseObserver.onNext(MarketDataResponse.newBuilder()
            .setSubscribeOrderBookResponse(SubscribeOrderBookResponse.newBuilder()
              .addOrderBookSubscriptions(OrderBookSubscription.newBuilder()
                .setInstrumentUid(instrumentUid)
                .setTicker(ticker)
                .setClassCode(classCode)
                .setFigi(figi)
                .setDepth(depth)
                .setOrderBookType(OrderBookType.ORDERBOOK_TYPE_EXCHANGE)
                .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
                .setSubscriptionStatus(SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
                .build())
              .build())
            .build());
          // Подписка оформлена только на биржевой стакан - пуш по дилерскому стакану приходить не должен
          responseObserver.onNext(MarketDataResponse.newBuilder()
            .setOrderbook(OrderBook.newBuilder()
              .setInstrumentUid(instrumentUid)
              .setTicker(ticker)
              .setClassCode(classCode)
              .setFigi(figi)
              .setDepth(depth)
              .setOrderBookType(OrderBookType.ORDERBOOK_TYPE_DEALER)
              .build())
            .build());
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      }));

    var factoryWithConfig = createStubFactory();
    var streamFactory = StreamServiceStubFactory.create(factoryWithConfig._1());
    var streamManagerFactory = StreamManagerFactory.create(streamFactory);
    var executorService = Executors.newFixedThreadPool(8);
    var scheduledExecutorService = Executors.newScheduledThreadPool(2);
    var manager = streamManagerFactory.newMarketDataStreamManager(executorService, scheduledExecutorService);

    try {
      manager.subscribeOrderBooks(
        Set.of(new Instrument(instrumentUid, depth, OrderBookType.ORDERBOOK_TYPE_EXCHANGE)),
        orderBook -> orderBooksReceived.add(orderBook.getInstrumentUid())
      );
      manager.start();

      Awaitility.await()
        .pollDelay(Duration.ofMillis(500))
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertThat(orderBooksReceived).isEmpty());
    } finally {
      manager.shutdown();
      executorService.shutdownNow();
      scheduledExecutorService.shutdownNow();
    }
  }
}
