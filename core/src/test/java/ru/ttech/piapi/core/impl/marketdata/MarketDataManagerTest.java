package ru.ttech.piapi.core.impl.marketdata;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.awaitility.Awaitility;
import org.grpcmock.junit5.GrpcMockExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.tinkoff.piapi.contract.v1.Candle;
import ru.tinkoff.piapi.contract.v1.CandleSubscription;
import ru.tinkoff.piapi.contract.v1.GetCandlesRequest;
import ru.tinkoff.piapi.contract.v1.GetCandlesResponse;
import ru.tinkoff.piapi.contract.v1.HistoricCandle;
import ru.tinkoff.piapi.contract.v1.MarketDataRequest;
import ru.tinkoff.piapi.contract.v1.MarketDataResponse;
import ru.tinkoff.piapi.contract.v1.MarketDataServiceGrpc;
import ru.tinkoff.piapi.contract.v1.MarketDataStreamServiceGrpc;
import ru.tinkoff.piapi.contract.v1.SubscribeCandlesResponse;
import ru.tinkoff.piapi.contract.v1.SubscriptionAction;
import ru.tinkoff.piapi.contract.v1.SubscriptionInterval;
import ru.tinkoff.piapi.contract.v1.SubscriptionStatus;
import ru.ttech.piapi.core.connector.GrpcStubBaseTest;
import ru.ttech.piapi.core.connector.streaming.StreamServiceStubFactory;
import ru.ttech.piapi.core.helpers.TimeMapper;
import ru.ttech.piapi.core.impl.marketdata.subscription.CandleSubscriptionSpec;
import ru.ttech.piapi.core.impl.marketdata.subscription.Instrument;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.grpcmock.GrpcMock.*;

@ExtendWith(GrpcMockExtension.class)
public class MarketDataManagerTest extends GrpcStubBaseTest {

  private static final String UID = "instrumentUid";

  @SneakyThrows
  @Test
  void testLiveDuringRecoveryKeepsOrder() {
    var t0 = LocalDateTime.of(2026, 2, 11, 10, 0);
    var t1 = t0.plusMinutes(1);
    var t2 = t0.plusMinutes(2);
    var t3 = t0.plusMinutes(3);
    var t4 = t0.plusMinutes(4);
    var t5 = t0.plusMinutes(5);

    var receivedTimes = new CopyOnWriteArrayList<LocalDateTime>();
    var subscribeCounter = new AtomicInteger(0);

    var successSubscriptionResponse = MarketDataResponse.newBuilder()
      .setSubscribeCandlesResponse(
        SubscribeCandlesResponse.newBuilder()
          .addCandlesSubscriptions(
            CandleSubscription.newBuilder()
              .setInstrumentUid(UID)
              .setInterval(SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
              .setSubscriptionAction(SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE)
              .setSubscriptionStatus(SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS)
              .build()
          )
          .build()
      )
      .build();

    stubFor(bidiStreamingMethod(MarketDataStreamServiceGrpc.getMarketDataStreamMethod())
      .willProxyTo(responseObserver -> new StreamObserver<>() {

        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        volatile boolean heartbeatStarted = false;

        @Override
        public void onNext(MarketDataRequest req) {
          if (!req.hasSubscribeCandlesRequest()) {
            return;
          }

          var n = subscribeCounter.incrementAndGet();

          responseObserver.onNext(successSubscriptionResponse);

          if (n == 1) {
            responseObserver.onNext(live(UID, SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE, t0));
            responseObserver.onNext(live(UID, SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE, t1));
            responseObserver.onError(new StatusException(Status.UNAVAILABLE));
            return;
          }

          if (n == 2) {
            if (!heartbeatStarted) {
              heartbeatStarted = true;
              scheduler.scheduleAtFixedRate(
                () -> responseObserver.onNext(
                  live(
                    UID,
                    SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE,
                    LocalDateTime.now()
                  )
                ),
                0,
                200,
                TimeUnit.MILLISECONDS
              );
            }

            responseObserver.onNext(live(UID, SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE, t5));
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

    var getCandlesResponse = GetCandlesResponse.newBuilder()
      .addAllCandles(List.of(
        historic(t2, true),
        historic(t3, true),
        historic(t4, true)
      ))
      .build();

    stubFor(unaryMethod(MarketDataServiceGrpc.getGetCandlesMethod())
      .willReturn(getCandlesResponse));

    var factoryWithConfig = createStubFactory();
    var factory = factoryWithConfig._1();
    var streamFactory = StreamServiceStubFactory.create(factory);

    var executor = Executors.newSingleThreadExecutor();
    var scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    var managerFactory = ru.ttech.piapi.core.connector.streaming.StreamManagerFactory.create(streamFactory);
    var streamManager = managerFactory.newMarketDataStreamManager(executor, scheduledExecutor);

    var mdm = new MarketDataManager(streamManager);

    var instrument = new Instrument(UID, SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE);

    var spec = new CandleSubscriptionSpec(
      GetCandlesRequest.CandleSource.CANDLE_SOURCE_INCLUDE_WEEKEND,
      true
    );

    mdm.subscribeCandles(Set.of(instrument), spec, cw -> receivedTimes.add(cw.getTime()));

    mdm.start();

    Awaitility.await()
      .atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> assertThat(receivedTimes).contains(t0, t1, t2, t3, t4, t5));

    assertThat(receivedTimes)
      .filteredOn(t -> !t.isAfter(t5))
      .containsExactly(t0, t1, t2, t3, t4, t5);

    mdm.shutdown();
    executor.shutdownNow();
    scheduledExecutor.shutdownNow();
  }

  private static MarketDataResponse live(String uid, SubscriptionInterval interval, LocalDateTime time) {
    var candle = Candle.newBuilder()
      .setInstrumentUid(uid)
      .setInterval(interval)
      .setTime(TimeMapper.localDateTimeToTimestamp(time))
      .build();

    return MarketDataResponse.newBuilder()
      .setCandle(candle)
      .build();
  }

  private static HistoricCandle historic(LocalDateTime time, boolean complete) {
    return HistoricCandle.newBuilder()
      .setTime(TimeMapper.localDateTimeToTimestamp(time))
      .setIsComplete(complete)
      .build();
  }
}
