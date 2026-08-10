package ru.ttech.piapi.core.impl.marketdata;

import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.awaitility.Awaitility;
import org.grpcmock.junit5.GrpcMockExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.tinkoff.piapi.contract.v1.Candle;
import ru.tinkoff.piapi.contract.v1.CandleSubscription;
import ru.tinkoff.piapi.contract.v1.MarketDataRequest;
import ru.tinkoff.piapi.contract.v1.MarketDataResponse;
import ru.tinkoff.piapi.contract.v1.MarketDataStreamServiceGrpc;
import ru.tinkoff.piapi.contract.v1.SubscribeCandlesResponse;
import ru.tinkoff.piapi.contract.v1.SubscribeTradesResponse;
import ru.tinkoff.piapi.contract.v1.SubscriptionAction;
import ru.tinkoff.piapi.contract.v1.SubscriptionInterval;
import ru.tinkoff.piapi.contract.v1.SubscriptionStatus;
import ru.tinkoff.piapi.contract.v1.Trade;
import ru.tinkoff.piapi.contract.v1.TradeSourceType;
import ru.tinkoff.piapi.contract.v1.TradeSubscription;
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
}
