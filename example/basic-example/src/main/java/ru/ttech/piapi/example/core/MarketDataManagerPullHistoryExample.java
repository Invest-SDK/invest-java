package ru.ttech.piapi.example.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.piapi.contract.v1.InstrumentIdType;
import ru.tinkoff.piapi.contract.v1.InstrumentRequest;
import ru.tinkoff.piapi.contract.v1.InstrumentsServiceGrpc;
import ru.tinkoff.piapi.contract.v1.SubscriptionInterval;
import ru.ttech.piapi.core.connector.ConnectorConfiguration;
import ru.ttech.piapi.core.connector.ServiceStubFactory;
import ru.ttech.piapi.core.connector.streaming.StreamManagerFactory;
import ru.ttech.piapi.core.connector.streaming.StreamServiceStubFactory;
import ru.ttech.piapi.core.connector.streaming.listeners.OnNextListener;
import ru.ttech.piapi.core.impl.marketdata.MarketDataManager;
import ru.ttech.piapi.core.impl.marketdata.subscription.CandleSubscriptionSpec;
import ru.ttech.piapi.core.impl.marketdata.subscription.Instrument;
import ru.ttech.piapi.core.impl.marketdata.wrapper.CandleWrapper;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class MarketDataManagerPullHistoryExample {
    private static final Logger logger = LoggerFactory.getLogger(MarketDataManagerPullHistoryExample.class);

    public static void main(String[] args) {
      var configuration = ConnectorConfiguration.loadPropertiesFromResources("invest.properties");
      var unaryServiceFactory = ServiceStubFactory.create(configuration);
      var instrumentsService = unaryServiceFactory.newSyncService(InstrumentsServiceGrpc::newBlockingStub);
      var instrumentsMap = Map.of(
              "SBER", "TQBR",
              "YDEX", "TQBR",
              "SBERP", "TQBR"
      );
      var instruments = instrumentsMap.entrySet().stream().map(entry -> {
        var request = InstrumentRequest.newBuilder()
          .setIdType(InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER)
          .setId(entry.getKey())
          .setClassCode(entry.getValue())
          .build();
        var response = instrumentsService.callSyncMethod(
          stub -> stub.shareBy(request)
        );
        var share = response.getInstrument();
        return new Instrument(share.getUid(), SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE);
      }).collect(Collectors.toSet());

      var streamServiceFactory = StreamServiceStubFactory.create(unaryServiceFactory);
      var streamManagerFactory = StreamManagerFactory.create(streamServiceFactory);
      var executorService = Executors.newCachedThreadPool();
      var scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
      var marketDataStreamManager = streamManagerFactory.newMarketDataStreamManager(executorService, scheduledExecutorService);
      var marketDataManager = new MarketDataManager(marketDataStreamManager);

      OnNextListener<CandleWrapper> listener = candle ->
        logger.info("New candle incoming for instrument: {}",
          candle.getTicker() + "_" + candle.getClassCode() + " "
          + candle.getTime() + " history " + candle.isHistory());
      var subscriptionSpec = new CandleSubscriptionSpec();
      logger.info("Подписываемся");
      marketDataManager.subscribeCandles(instruments, subscriptionSpec, listener);
      marketDataStreamManager.start();
      try {
        Thread.sleep(600_000);
        marketDataManager.unsubscribeCandles(instruments, subscriptionSpec);
        Thread.currentThread().join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        marketDataManager.shutdown();
      }
    }
}
