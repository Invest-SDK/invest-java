package ru.ttech.piapi.core.connector.stream;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.awaitility.Awaitility;
import org.grpcmock.junit5.GrpcMockExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.tinkoff.piapi.contract.v1.Candle;
import ru.tinkoff.piapi.contract.v1.CandleInstrument;
import ru.tinkoff.piapi.contract.v1.CandleSubscription;
import ru.tinkoff.piapi.contract.v1.LastPrice;
import ru.tinkoff.piapi.contract.v1.LastPriceSubscription;
import ru.tinkoff.piapi.contract.v1.MarketDataRequest;
import ru.tinkoff.piapi.contract.v1.MarketDataResponse;
import ru.tinkoff.piapi.contract.v1.MarketDataStreamServiceGrpc;
import ru.tinkoff.piapi.contract.v1.SubscribeCandlesRequest;
import ru.tinkoff.piapi.contract.v1.SubscribeCandlesResponse;
import ru.tinkoff.piapi.contract.v1.SubscribeLastPriceResponse;
import ru.tinkoff.piapi.contract.v1.SubscriptionAction;
import ru.tinkoff.piapi.contract.v1.SubscriptionInterval;
import ru.tinkoff.piapi.contract.v1.SubscriptionStatus;
import ru.ttech.piapi.core.connector.GrpcStubBaseTest;
import ru.ttech.piapi.core.connector.streaming.StreamServiceStubFactory;
import ru.ttech.piapi.core.impl.marketdata.MarketDataStreamWrapperConfiguration;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.grpcmock.GrpcMock.bidiStreamingMethod;
import static org.grpcmock.GrpcMock.stubFor;
import static org.grpcmock.GrpcMock.times;
import static org.grpcmock.GrpcMock.verifyThat;

@ExtendWith(GrpcMockExtension.class)
public class MarketDataWrapperTest extends GrpcStubBaseTest {


  @SneakyThrows
  @Test
  void testMarketDataStreamReconnect() {
    var receivedUpdates = new AtomicInteger();
    var response = MarketDataResponse.newBuilder()
      .setCandle(Candle.getDefaultInstance())
      .build();
    var successSubscriptionResponse = MarketDataResponse.newBuilder()
      .setSubscribeCandlesResponse(SubscribeCandlesResponse.newBuilder()
        .addCandlesSubscriptions(CandleSubscription.newBuilder()
          .setInstrumentUid("instrumentUid")
          .setInterval(SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
          .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
          .setSubscriptionStatus(SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
          .build())
        .build())
      .build();
    var request = MarketDataRequest.newBuilder()
      .setSubscribeCandlesRequest(SubscribeCandlesRequest.newBuilder()
        .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
        .addInstruments(CandleInstrument.newBuilder()
          .setInstrumentId("instrumentUid")
          .setInterval(SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
          .build())
        .build())
      .build();
    stubFor(bidiStreamingMethod(MarketDataStreamServiceGrpc.getMarketDataStreamMethod())
      .withFirstRequest(request)
      .willProxyTo(responseObserver -> new StreamObserver<>() {
        @Override
        public void onNext(MarketDataRequest marketDataRequest) {
          try {
            responseObserver.onNext(successSubscriptionResponse);
            Thread.sleep(10);
            responseObserver.onNext(response);
            Thread.sleep(10);
            responseObserver.onNext(response);
            Thread.sleep(10);
            responseObserver.onError(new StatusException(Status.UNAVAILABLE));
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
    var factory = factoryWithConfig._1();
    var streamFactory = StreamServiceStubFactory.create(factory);
    var executorService = Executors.newSingleThreadScheduledExecutor();
    var wrapper = streamFactory.newResilienceMarketDataStream(MarketDataStreamWrapperConfiguration.builder(executorService)
      .addOnCandleListener(candle -> receivedUpdates.incrementAndGet())
      .build());
    wrapper.newCall(request);

    Awaitility.await()
      .atMost(Duration.ofSeconds(2))
      .untilAsserted(() -> assertThat(receivedUpdates.get()).isEqualTo(6));
    verifyThat(MarketDataStreamServiceGrpc.getMarketDataStreamMethod(), times(3));
    wrapper.disconnect();
    executorService.shutdownNow();
  }

  @SneakyThrows
  @Test
  void testMarketDataStreamNotReconnect() {
    var receivedUpdates = new AtomicInteger();
    var lastPriceResponse = MarketDataResponse.newBuilder()
      .setLastPrice(LastPrice.newBuilder()
        .setInstrumentUid("instrumentUid")
        .build())
      .build();
    var invalidSubscriptionResponse = MarketDataResponse.newBuilder()
      .setSubscribeCandlesResponse(SubscribeCandlesResponse.newBuilder()
        .addCandlesSubscriptions(CandleSubscription.newBuilder()
          .setInstrumentUid("instrumentUid")
          .setInterval(SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
          .setSubscriptionStatus(SubscriptionStatus.SUBSCRIPTION_STATUS_INSTRUMENT_NOT_FOUND)
          .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
          .build())
        .build())
      .build();
    var request = MarketDataRequest.newBuilder()
      .setSubscribeCandlesRequest(SubscribeCandlesRequest.newBuilder()
        .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
        .addInstruments(CandleInstrument.newBuilder()
          .setInstrumentId("instrumentUid")
          .setInterval(SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
          .build())
        .build())
      .build();
    stubFor(bidiStreamingMethod(MarketDataStreamServiceGrpc.getMarketDataStreamMethod())
      .withFirstRequest(request)
      .willProxyTo(responseObserver -> new StreamObserver<>() {
        @Override
        public void onNext(MarketDataRequest marketDataRequest) {
          try {
            responseObserver.onNext(invalidSubscriptionResponse);
            Thread.sleep(10);
            responseObserver.onNext(lastPriceResponse);
            Thread.sleep(10);
            responseObserver.onError(new StatusException(Status.UNAVAILABLE));
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
    var factory = factoryWithConfig._1();
    var streamFactory = StreamServiceStubFactory.create(factory);
    var executorService = Executors.newSingleThreadScheduledExecutor();
    var wrapper = streamFactory.newResilienceMarketDataStream(MarketDataStreamWrapperConfiguration.builder(executorService)
      .addOnCandleListener(candle -> receivedUpdates.incrementAndGet())
      .build());
    wrapper.newCall(request);

    Awaitility.await().pollDelay(Duration.ofSeconds(1))
        .untilAsserted(() -> assertThat(receivedUpdates.get()).isEqualTo(0));
    verifyThat(MarketDataStreamServiceGrpc.getMarketDataStreamMethod());
    wrapper.disconnect();
    executorService.shutdownNow();
  }

  @SneakyThrows
  @Test
  void testMarketDataStreamReconnectToleratesStaleSubscriptionResult() {
    var receivedUpdates = new AtomicInteger();
    var reconnectedCount = new AtomicInteger();
    var candle = MarketDataResponse.newBuilder().setCandle(Candle.getDefaultInstance()).build();
    var successSubscriptionResponse = MarketDataResponse.newBuilder()
      .setSubscribeCandlesResponse(SubscribeCandlesResponse.newBuilder()
        .addCandlesSubscriptions(CandleSubscription.newBuilder()
          .setInstrumentUid("instrumentUid")
          .setInterval(SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
          .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
          .setSubscriptionStatus(SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
          .build())
        .build())
      .build();
    var staleUnrelatedResponse = MarketDataResponse.newBuilder()
      .setSubscribeLastPriceResponse(SubscribeLastPriceResponse.newBuilder()
        .addLastPriceSubscriptions(LastPriceSubscription.newBuilder()
          .setInstrumentUid("someOtherInstrument")
          .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_UNSUBSCRIBE)
          .setSubscriptionStatus(SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
          .build())
        .build())
      .build();
    var request = MarketDataRequest.newBuilder()
      .setSubscribeCandlesRequest(SubscribeCandlesRequest.newBuilder()
        .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
        .addInstruments(CandleInstrument.newBuilder()
          .setInstrumentId("instrumentUid")
          .setInterval(SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
          .build())
        .build())
      .build();
    var connectionAttempt = new AtomicInteger();
    stubFor(bidiStreamingMethod(MarketDataStreamServiceGrpc.getMarketDataStreamMethod())
      .withFirstRequest(request)
      .willProxyTo(responseObserver -> new StreamObserver<>() {
        final int attempt = connectionAttempt.incrementAndGet();

        @Override
        public void onNext(MarketDataRequest marketDataRequest) {
          if (!marketDataRequest.hasSubscribeCandlesRequest()) {
            return;
          }
          try {
            if (attempt == 1) {
              responseObserver.onNext(successSubscriptionResponse);
              Thread.sleep(10);
              responseObserver.onNext(candle);
              Thread.sleep(10);
              responseObserver.onError(new StatusException(Status.UNAVAILABLE));
            } else {
              responseObserver.onNext(staleUnrelatedResponse);
              Thread.sleep(10);
              responseObserver.onNext(successSubscriptionResponse);
              Thread.sleep(10);
              responseObserver.onNext(candle);
            }
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
    var factory = factoryWithConfig._1();
    var streamFactory = StreamServiceStubFactory.create(factory);
    var executorService = Executors.newSingleThreadScheduledExecutor();
    var wrapper = streamFactory.newResilienceMarketDataStream(MarketDataStreamWrapperConfiguration.builder(executorService)
      .addOnCandleListener(c -> receivedUpdates.incrementAndGet())
      .addOnConnectListener(event -> reconnectedCount.incrementAndGet())
      .build());
    try {
      wrapper.newCall(request);

      Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertThat(receivedUpdates.get()).isEqualTo(2));
      Awaitility.await()
        .atMost(Duration.ofSeconds(1))
        .untilAsserted(() -> assertThat(reconnectedCount.get()).isGreaterThanOrEqualTo(1));
    } finally {
      wrapper.disconnect();
      executorService.shutdownNow();
    }
  }

  @SneakyThrows
  @Test
  void testMarketDataStreamReconnectRetriesUnconfirmedSubscription() {
    var receivedUpdates = new AtomicInteger();
    var reconnectedCount = new AtomicInteger();
    var candle = MarketDataResponse.newBuilder().setCandle(Candle.getDefaultInstance()).build();
    var successSubscriptionResponse = MarketDataResponse.newBuilder()
      .setSubscribeCandlesResponse(SubscribeCandlesResponse.newBuilder()
        .addCandlesSubscriptions(CandleSubscription.newBuilder()
          .setInstrumentUid("instrumentUid")
          .setInterval(SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
          .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
          .setSubscriptionStatus(SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
          .build())
        .build())
      .build();
    var request = MarketDataRequest.newBuilder()
      .setSubscribeCandlesRequest(SubscribeCandlesRequest.newBuilder()
        .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
        .addInstruments(CandleInstrument.newBuilder()
          .setInstrumentId("instrumentUid")
          .setInterval(SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
          .build())
        .build())
      .build();
    var connectionAttempt = new AtomicInteger();
    stubFor(bidiStreamingMethod(MarketDataStreamServiceGrpc.getMarketDataStreamMethod())
      .withFirstRequest(request)
      .willProxyTo(responseObserver -> new StreamObserver<>() {
        final int attempt = connectionAttempt.incrementAndGet();
        final AtomicInteger requestsInConnection = new AtomicInteger();

        @Override
        public void onNext(MarketDataRequest marketDataRequest) {
          if (!marketDataRequest.hasSubscribeCandlesRequest()) {
            return;
          }
          int requestNumber = requestsInConnection.incrementAndGet();
          try {
            if (attempt == 1) {
              responseObserver.onNext(successSubscriptionResponse);
              Thread.sleep(10);
              responseObserver.onNext(candle);
              Thread.sleep(10);
              responseObserver.onError(new StatusException(Status.UNAVAILABLE));
            } else if (requestNumber > 1) {
              responseObserver.onNext(successSubscriptionResponse);
              Thread.sleep(10);
              responseObserver.onNext(candle);
            }
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
    var factory = factoryWithConfig._1();
    var streamFactory = StreamServiceStubFactory.create(factory);
    var executorService = Executors.newSingleThreadScheduledExecutor();
    var wrapper = streamFactory.newResilienceMarketDataStream(MarketDataStreamWrapperConfiguration.builder(executorService)
      .addOnCandleListener(c -> receivedUpdates.incrementAndGet())
      .addOnConnectListener(event -> reconnectedCount.incrementAndGet())
      .build());
    try {
      wrapper.newCall(request);

      Awaitility.await()
        .atMost(Duration.ofSeconds(1))
        .untilAsserted(() -> assertThat(receivedUpdates.get()).isEqualTo(1));
      Awaitility.await()
        .atMost(Duration.ofSeconds(3))
        .untilAsserted(() -> assertThat(receivedUpdates.get()).isEqualTo(2));
      Awaitility.await()
        .atMost(Duration.ofSeconds(1))
        .untilAsserted(() -> assertThat(reconnectedCount.get()).isGreaterThanOrEqualTo(1));
    } finally {
      wrapper.disconnect();
      executorService.shutdownNow();
    }
  }

  @SneakyThrows
  @Test
  void testMarketDataStreamGivesUpAfterMaxRejectedResubscriptionAttempts() {
    var receivedUpdates = new AtomicInteger();
    var reconnectedCount = new AtomicInteger();
    var candle = MarketDataResponse.newBuilder().setCandle(Candle.getDefaultInstance()).build();
    var successSubscriptionResponse = MarketDataResponse.newBuilder()
      .setSubscribeCandlesResponse(SubscribeCandlesResponse.newBuilder()
        .addCandlesSubscriptions(CandleSubscription.newBuilder()
          .setInstrumentUid("instrumentUid")
          .setInterval(SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
          .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
          .setSubscriptionStatus(SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
          .build())
        .build())
      .build();
    var rejectedSubscriptionResponse = MarketDataResponse.newBuilder()
      .setSubscribeCandlesResponse(SubscribeCandlesResponse.newBuilder()
        .addCandlesSubscriptions(CandleSubscription.newBuilder()
          .setInstrumentUid("instrumentUid")
          .setInterval(SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
          .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
          .setSubscriptionStatus(SubscriptionStatus.SUBSCRIPTION_STATUS_INSTRUMENT_NOT_FOUND)
          .build())
        .build())
      .build();
    var request = MarketDataRequest.newBuilder()
      .setSubscribeCandlesRequest(SubscribeCandlesRequest.newBuilder()
        .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
        .addInstruments(CandleInstrument.newBuilder()
          .setInstrumentId("instrumentUid")
          .setInterval(SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
          .build())
        .build())
      .build();
    var connectionAttempt = new AtomicInteger();
    var rejectionsSentCount = new AtomicInteger();
    stubFor(bidiStreamingMethod(MarketDataStreamServiceGrpc.getMarketDataStreamMethod())
      .withFirstRequest(request)
      .willProxyTo(responseObserver -> new StreamObserver<>() {
        final int attempt = connectionAttempt.incrementAndGet();

        @Override
        public void onNext(MarketDataRequest marketDataRequest) {
          if (!marketDataRequest.hasSubscribeCandlesRequest()) {
            return;
          }
          try {
            if (attempt == 1) {
              responseObserver.onNext(successSubscriptionResponse);
              Thread.sleep(10);
              responseObserver.onNext(candle);
              Thread.sleep(10);
              responseObserver.onError(new StatusException(Status.UNAVAILABLE));
            } else {
              rejectionsSentCount.incrementAndGet();
              responseObserver.onNext(rejectedSubscriptionResponse);
            }
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
    var factory = factoryWithConfig._1();
    var streamFactory = StreamServiceStubFactory.create(factory);
    var executorService = Executors.newSingleThreadScheduledExecutor();
    var wrapper = streamFactory.newResilienceMarketDataStream(MarketDataStreamWrapperConfiguration.builder(executorService)
      .addOnCandleListener(c -> receivedUpdates.incrementAndGet())
      .addOnConnectListener(event -> reconnectedCount.incrementAndGet())
      .build());
    try {
      wrapper.newCall(request);

      Awaitility.await()
        .atMost(Duration.ofSeconds(1))
        .untilAsserted(() -> assertThat(receivedUpdates.get()).isEqualTo(1));
      Awaitility.await()
        .atMost(Duration.ofSeconds(3))
        .untilAsserted(() -> assertThat(rejectionsSentCount.get()).isEqualTo(3));
      Awaitility.await().pollDelay(Duration.ofMillis(500))
        .untilAsserted(() -> assertThat(rejectionsSentCount.get()).isEqualTo(3));
      assertThat(reconnectedCount.get()).isEqualTo(0);
    } finally {
      wrapper.disconnect();
      executorService.shutdownNow();
    }
  }
}
