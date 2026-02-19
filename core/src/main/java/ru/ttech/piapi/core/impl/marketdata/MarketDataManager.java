package ru.ttech.piapi.core.impl.marketdata;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.piapi.contract.v1.Candle;
import ru.tinkoff.piapi.contract.v1.CandleInterval;
import ru.tinkoff.piapi.contract.v1.GetCandlesRequest;
import ru.tinkoff.piapi.contract.v1.HistoricCandle;
import ru.tinkoff.piapi.contract.v1.MarketDataServiceGrpc;
import ru.tinkoff.piapi.contract.v1.TradeSourceType;
import ru.ttech.piapi.core.connector.ServiceStubFactory;
import ru.ttech.piapi.core.connector.resilience.ResilienceAsyncStubWrapper;
import ru.ttech.piapi.core.connector.resilience.ResilienceConfiguration;
import ru.ttech.piapi.core.connector.streaming.listeners.OnNextListener;
import ru.ttech.piapi.core.helpers.TimeMapper;
import ru.ttech.piapi.core.impl.marketdata.subscription.CandleSubscriptionSpec;
import ru.ttech.piapi.core.impl.marketdata.subscription.Instrument;
import ru.ttech.piapi.core.impl.marketdata.wrapper.CandleWrapper;
import ru.ttech.piapi.core.impl.marketdata.wrapper.LastPriceWrapper;
import ru.ttech.piapi.core.impl.marketdata.wrapper.OpenInterestWrapper;
import ru.ttech.piapi.core.impl.marketdata.wrapper.OrderBookWrapper;
import ru.ttech.piapi.core.impl.marketdata.wrapper.TradeWrapper;
import ru.ttech.piapi.core.impl.marketdata.wrapper.TradingStatusWrapper;
import ru.ttech.piapi.core.impl.wrapper.ResponseWrapper;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Менеджер стримов рыночных данных с поддержкой восстановления пропущенных свечей.
 */
public class MarketDataManager {

    private static final Logger logger = LoggerFactory.getLogger(MarketDataManager.class);

    @Getter
    private final MarketDataStreamManager streamManager;
    private final ResilienceAsyncStubWrapper<MarketDataServiceGrpc.MarketDataServiceStub> marketDataService;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final List<OnNextListener<CandleWrapper>> replay = Collections.synchronizedList(new ArrayList<>());
    private final ConcurrentMap<InstrIdResponseType, NavigableMap<LocalDateTime, CandleWrapper>> pending = new ConcurrentHashMap<>();
    private final ConcurrentMap<InstrIdResponseType, AtomicBoolean> recovering = new ConcurrentHashMap<>();


    public MarketDataManager(MarketDataStreamManager streamManager) {
        this.streamManager = streamManager;

        ServiceStubFactory serviceFactory = streamManager.getStreamFactory().getServiceStubFactory();
        this.marketDataService = serviceFactory.newResilienceAsyncService(
                MarketDataServiceGrpc::newStub,
                ResilienceConfiguration.builder(executorService, serviceFactory.getConfiguration()).build()
        );

        streamManager.addOnConnectListener((onConnectStreamInfo) ->
                recoverMissedCandles(onConnectStreamInfo.id()));
        streamManager.addOnConnectListenersBlockInstruments(onConnectStreamInfo ->
                blockRecoverCandles(onConnectStreamInfo.id()));
    }

    /**
     * Запускает поток маркет-данных.
     * <p>
     * Делегирует запуск {@link MarketDataStreamManager#start()}.
     * Инициируется восстановление пропущенных свечей.
     */
    public void start() {
        streamManager.start();
    }

    /**
     * Останавливает поток маркет-данных и освобождает ресурсы.
     * <p>
     */
    public void shutdown() {
        try {
            streamManager.shutdown();
        } finally {
            executorService.shutdownNow();
        }
    }

    private void blockRecoverCandles(UUID wrapperId) {
        var subscriptions = streamManager.getSubscriptionsByWrapperUuid(wrapperId);
        subscriptions.forEach(subscription -> {
            if (subscription.getInstrument().getSubscriptionInterval() != null) {
                var instrIdResponseType = new InstrIdResponseType(subscription.getInstrument().getInstrumentId().id(),
                        MarketDataResponseType.CANDLE);
                recovering.putIfAbsent(instrIdResponseType, new AtomicBoolean(true));
            }
        });
    }

    /**
     * Метод для подписки на свечи по списку инструментов.
     * <p>
     * <p>Пример:
     * <pre>{@code
     * marketDataManager.subscribeCandles(
     *     instruments,
     *     new CandleSubscriptionSpec(),
     *     candle -> logger.info("New candle: {}", candle.getTime())
     * );
     * }</pre>
     *
     * @param instruments  список инструментов {@link Instrument}
     * @param spec         параметры подписки {@link CandleSubscriptionSpec}
     * @param onCandleListener пользовательский обработчик свечей
     */
    public void subscribeCandles(
            Set<Instrument> instruments,
            CandleSubscriptionSpec spec,
            OnNextListener<CandleWrapper> onCandleListener
    ) {
        addWrapperListener(replay, onCandleListener);
        streamManager.subscribeCandles(instruments, spec, candle -> {
            var key = InstrIdResponseType.of(MarketDataResponseType.CANDLE, candle.getInstrumentUid());
            var isRecovering = recovering.getOrDefault(key, new AtomicBoolean(false)).get();

            if (isRecovering) {
                pending
                        .computeIfAbsent(key, k -> new ConcurrentSkipListMap<>())
                        .put(candle.getTime(), candle);
            } else {
                replay.forEach(listener -> listener.onNext(candle));
            }
        });
    }

    /**
     * Метод для отписки от свечей по списку инструментов.
     * <p>
     * Удаляет внутренние структуры:
     * <ul>
     *   <li>листенер</li>
     *   <li>буфер ожидающих свечей</li>
     * </ul>
     *
     * @param instruments список инструментов {@link Instrument}
     * @param spec        параметры подписки {@link CandleSubscriptionSpec}
     */
    public void unsubscribeCandles(Set<Instrument> instruments, CandleSubscriptionSpec spec) {
        streamManager.unsubscribeCandles(instruments, spec);
        instruments.forEach(instrument -> {
            var key = InstrIdResponseType.of(MarketDataResponseType.CANDLE, instrument.getInstrumentId().id());
            pending.remove(key);
        });
    }

    /**
     * Метод для проверки наличия подписки на свечи по инструменту
     *
     * @param instrument Инструмент
     * @return true если активная подписка есть, false если нет
     */
    public boolean isSubscribedCandles(Instrument instrument) {
        return streamManager.isSubscribedCandles(instrument);
    }

    /**
     * Метод для подписки на последние цены по списку инструментов
     * <p>Пример:
     * <pre>{@code
     * marketDataStreamManager.subscribeLastPrices(
     *       availableInstruments,
     *       lastPrice -> logger.info("New last price incoming for instrument: {}", lastPrice.getInstrumentUid())
     *     );
     * }</pre>
     *
     * @param instruments список инструментов {@link Instrument}
     * @param listener    пользовательский листнер
     */
    public void subscribeLastPrices(Set<Instrument> instruments,
                                    OnNextListener<LastPriceWrapper> listener) {
        streamManager.subscribeLastPrices(instruments, listener);
    }

    /**
     * Метод для отписки от последних цен по списку инструментов
     *
     * @param instruments список инструментов {@link Instrument}
     */
    public void unsubscribeLastPrices(Set<Instrument> instruments) {
        streamManager.unsubscribeLastPrices(instruments);
    }

    /**
     * Метод для проверки наличия подписки на последнюю цену по инструменту
     *
     * @param instrument Инструмент
     * @return true если активная подписка есть, false если нет
     */
    public boolean isSubscribedLastPrice(Instrument instrument) {
        return streamManager.isSubscribedLastPrice(instrument);
    }

    /**
     * Метод для подписки на сделки по списку инструментов
     * <p>Пример:
     * <pre>{@code
     *     marketDataStreamManager.subscribeTrades(
     *       availableInstruments,
     *       TradeSourceType.TRADE_SOURCE_ALL,
     *       true,
     *       trade -> logger.info("New trade incoming for instrument: {}", trade.getInstrumentUid()),
     *       openInterest -> logger.info("New open interest incoming for instrument: {}", openInterest.getInstrumentUid())
     *     );
     * }</pre>
     *
     * @param instruments            список инструментов {@link Instrument}
     * @param tradeSourceType        тип источника сделок
     * @param withOpenInterest       флаг открытого интереса для фьючерсов
     * @param onTradeListener        листенер сделок
     * @param onOpenInterestListener листенер открытого интереса фьючерсов
     */
    public void subscribeTrades(Set<Instrument> instruments,
                                TradeSourceType tradeSourceType,
                                boolean withOpenInterest,
                                OnNextListener<TradeWrapper> onTradeListener,
                                OnNextListener<OpenInterestWrapper> onOpenInterestListener) {
        streamManager.subscribeTrades(instruments, tradeSourceType, withOpenInterest, onTradeListener, onOpenInterestListener);
    }

    /**
     * Метод для отписки от сделок по списку инструментов
     *
     * @param instruments     список инструментов {@link Instrument}
     * @param tradeSourceType тип источника сделок
     */
    public void unsubscribeTrades(Set<Instrument> instruments,
                                  boolean withOpenInterest,
                                  TradeSourceType tradeSourceType) {
        streamManager.unsubscribeTrades(instruments, withOpenInterest, tradeSourceType);
    }

    /**
     * Метод для проверки наличия подписки на сделки по инструменту
     *
     * @param instrument Инструмент
     * @return true если активная подписка есть, false если нет
     */
    public boolean isSubscribedTrades(Instrument instrument) {
        return streamManager.isSubscribedTrades(instrument);
    }

    /**
     * Метод для подписки на торговые стаканы по списку инструментов
     * <p>Пример:
     * <pre>{@code
     *     marketDataStreamManager.subscribeOrderBooks(
     *       availableInstruments,
     *       orderBook -> logger.info("New order book incoming for instrument: {}", orderBook.getInstrumentUid())
     *     );
     * }</pre>
     *
     * @param instruments         список инструментов {@link Instrument}
     * @param onOrderBookListener листенер торговых стаканов
     */
    public void subscribeOrderBooks(Set<Instrument> instruments,
                                    OnNextListener<OrderBookWrapper> onOrderBookListener) {
        streamManager.subscribeOrderBooks(instruments, onOrderBookListener);
    }

    /**
     * Метод для отписки от торговых стаканов по списку инструментов
     *
     * @param instruments список инструментов {@link Instrument}
     */
    public void unsubscribeOrderBooks(Set<Instrument> instruments) {
        streamManager.unsubscribeOrderBooks(instruments);
    }

    /**
     * Метод для проверки наличия подписки на торговый стакан по инструменту
     *
     * @param instrument Инструмент
     * @return true если активная подписка есть, false если нет
     */
    public boolean isSubscribedOrderBook(Instrument instrument) {
        return streamManager.isSubscribedOrderBook(instrument);
    }

    /**
     * Метод для подписки на статусы торгов по списку инструментов
     * <p>Пример:
     * <pre>{@code
     *     marketDataStreamManager.subscribeTradingStatuses(
     *       availableInstruments,
     *       tradingStatus -> logger.info("New trading status incoming for instrument: {}", tradingStatus.getInstrumentUid())
     *     );
     * }</pre>
     *
     * @param instruments             список инструментов {@link Instrument}
     * @param onTradingStatusListener листенер статусов торгов
     */
    public void subscribeTradingStatuses(Set<Instrument> instruments,
                                         OnNextListener<TradingStatusWrapper> onTradingStatusListener) {
        streamManager.subscribeTradingStatuses(instruments, onTradingStatusListener);
    }

    /**
     * Метод для отписки от статусов торгов по списку инструментов
     *
     * @param instruments список инструментов {@link Instrument}
     */
    public void unsubscribeTradingStatuses(Set<Instrument> instruments) {
        streamManager.unsubscribeTradingStatuses(instruments);
    }

    /**
     * Метод для проверки наличия подписки на статус торгов по инструменту
     *
     * @param instrument Инструмент
     * @return true если активная подписка есть, false если нет
     */
    public boolean isSubscribedTradingStatuses(Instrument instrument) {
        return streamManager.isSubscribedTradingStatuses(instrument);
    }

    /**
     * Асинхронно инициирует восстановление пропущенных свечей
     * после реконнекта стрима.
     * <p>
     */
    private void recoverMissedCandles(UUID wrapperId) {
        logger.info("Starting recovery for wrapper {}...", wrapperId);
        streamManager.setNeedToUpdateReceiveTimeByWrapperUuid(false, wrapperId);
        var subscriptions = streamManager.getSubscriptionsByWrapperUuid(wrapperId);
        var futures = subscriptions.stream()
                .map(subscriptionEntry -> {
                    if (subscriptionEntry.getInstrument().getSubscriptionInterval() != null) {
                                return recoverOne(
                                        subscriptionEntry.getInstrument(),
                                        subscriptionEntry.getLastReceiveTime(),
                                        wrapperId
                                );
                            }
                            return CompletableFuture.completedFuture(null);
                        }
                )
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures)
                .whenCompleteAsync((unused, throwable) -> {
                        if (throwable != null) {
                            logger.warn("Recovery failed for wrapper {}", wrapperId, throwable);
                        } else {
                            logger.info("Recovery finished for wrapper {}", wrapperId);
                        }
                  streamManager.setNeedToUpdateReceiveTimeByWrapperUuid(true, wrapperId);
                }, executorService);
    }

    /**
     * Восстанавливает пропущенные свечи для конкретной подписки.
     * <p>
     *
     * @return future завершения восстановления
     */
    private CompletableFuture<Void> recoverOne(Instrument instrument, LocalDateTime lastReceiveTime, UUID wrapperId) {
        if (lastReceiveTime == null) {
            return CompletableFuture.completedFuture(null);
        }
        var endTime = Instant.now()
                .atOffset(ZoneOffset.UTC)
                .toLocalDateTime().truncatedTo(ChronoUnit.SECONDS);

        var candleKey = InstrIdResponseType.of(MarketDataResponseType.CANDLE, instrument.getInstrumentId().id());
        recovering.computeIfAbsent(candleKey, k -> new AtomicBoolean()).set(true);
        pending.computeIfAbsent(candleKey, k -> new ConcurrentSkipListMap<>());
        var request = GetCandlesRequest.newBuilder()
                .setFrom(TimeMapper.localDateTimeToTimestamp(lastReceiveTime))
                .setTo(TimeMapper.localDateTimeToTimestamp(endTime))
                .setInstrumentId(instrument.getInstrumentId().id())
                .setInterval(CandleInterval.forNumber(instrument.getSubscriptionInterval().getNumber()))
                .build();

        var responseFuture = marketDataService.callAsyncMethod(
                MarketDataServiceGrpc.getGetCandlesMethod(),
                (stub, obs) -> stub.getCandles(request, obs)
        );

        return responseFuture
                .thenApplyAsync(response -> response.getCandlesList().stream()
                        .filter(HistoricCandle::getIsComplete)
                        .map(historicCandle -> toHistoryWrapper(historicCandle, instrument))
                        .sorted(Comparator.comparing(CandleWrapper::getTime))
                        .toList(), executorService)
                .thenAcceptAsync(history -> {
                    for (var cw : history) {
                        replay.forEach(replayListener -> replayListener.onNext(cw));
                    }

                    var buffered = pending.get(candleKey);
                    if (buffered != null && !buffered.isEmpty()) {
                        buffered.values()
                                .stream()
                                .map(this::toHistoryWrapper)
                                .forEach(historyWrapper -> replay.forEach(
                                        replayListener ->
                                                replayListener.onNext(historyWrapper)));

                        buffered.clear();
                    }
                    streamManager.setLastReceiveTimeByWrapperUuid(instrument, endTime, wrapperId);

                    recovering.get(candleKey).set(false);
                }, executorService).exceptionally(ex -> {
                    var state = recovering.get(candleKey);
                    if (state != null) {
                        state.set(false);
                    }
                    throw new CompletionException(ex);
                });
    }

    private CandleWrapper toHistoryWrapper(HistoricCandle historicCandle, Instrument instrument) {
        var candleFigi = instrument.getInstrumentId().getIds().get(Instrument.IdType.FIGI);
        var candleTickerClassCode = instrument.getInstrumentId().getIds().get(Instrument.IdType.TICKER_CLASSCODE);
        var splitCandleTickerClassCode = splitTickerClassCode(candleTickerClassCode);
        var candleTicker = splitCandleTickerClassCode.ticker;
        var candleClassCode = splitCandleTickerClassCode.classCode;

        var candle = Candle.newBuilder()
                .setInstrumentUid(instrument.getInstrumentId().id())
                .setInterval(instrument.getSubscriptionInterval())
                .setTime(historicCandle.getTime())
                .setOpen(historicCandle.getOpen())
                .setHigh(historicCandle.getHigh())
                .setLow(historicCandle.getLow())
                .setClose(historicCandle.getClose())
                .setVolume(historicCandle.getVolume())
                .setVolumeBuy(historicCandle.getVolumeBuy())
                .setVolumeSell(historicCandle.getVolumeSell())
                .setFigi(candleFigi)
                .setTicker(candleTicker)
                .setClassCode(candleClassCode)
                .setCandleSourceType(historicCandle.getCandleSource());
        return new CandleWrapper(candle.build(), true);
    }

    private CandleWrapper toHistoryWrapper(CandleWrapper candleWrapper) {
        return new CandleWrapper(candleWrapper.getOriginal(), true);
    }

    private record InstrIdResponseType(
            String instrumentId,
            MarketDataResponseType responseType
    ) {
        static InstrIdResponseType of(MarketDataResponseType responseType, String instrumentId) {
            return new InstrIdResponseType(instrumentId, responseType
            );
        }
    }

    private record TickerClassCode(String ticker, String classCode) {
    }

    private TickerClassCode splitTickerClassCode(String string) {
        int i = (string == null) ? -1 : string.indexOf('_');
        return (i < 0)
                ? new TickerClassCode(string != null ? string : "", "")
                : new TickerClassCode(string.substring(0, i), string.substring(i + 1));
    }

    protected <T extends ResponseWrapper<?>> void addWrapperListener(
            List<OnNextListener<T>> listeners,
            OnNextListener<T> listener
    ) {
        if (!listeners.contains(listener)) {
            listeners.add(listener);
        }
    }
}
