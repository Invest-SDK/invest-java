package ru.ttech.piapi.core.impl.marketdata;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.piapi.contract.v1.Candle;
import ru.tinkoff.piapi.contract.v1.CandleInterval;
import ru.tinkoff.piapi.contract.v1.CandleSource;
import ru.tinkoff.piapi.contract.v1.GetCandlesRequest;
import ru.tinkoff.piapi.contract.v1.HistoricCandle;
import ru.tinkoff.piapi.contract.v1.MarketDataServiceGrpc;
import ru.tinkoff.piapi.contract.v1.SubscriptionInterval;
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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Менеджер стримов рыночных данных с поддержкой восстановления пропущенных свечей.
 */
public class MarketDataManager {

    private static final Logger logger = LoggerFactory.getLogger(MarketDataManager.class);

    @Getter
    private final MarketDataStreamManager streamManager;
    private final ResilienceAsyncStubWrapper<MarketDataServiceGrpc.MarketDataServiceStub> marketDataService;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean recoveryInProgress = new AtomicBoolean(false);
    private final AtomicReference<CompletableFuture<Void>> recoveryGate =
            new AtomicReference<>(CompletableFuture.completedFuture(null));
    private final ConcurrentMap<CandleKey, OnNextListener<CandleWrapper>> replay = new ConcurrentHashMap<>();
    private final ConcurrentMap<CandleKey, LocalDateTime> lastTime = new ConcurrentHashMap<>();
    private final ConcurrentMap<CandleKey, NavigableMap<LocalDateTime, CandleWrapper>> pending = new ConcurrentHashMap<>();
    private final AtomicBoolean firstConnectSeen = new AtomicBoolean(false);
    private final ConcurrentMap<CandleKey, CandleMeta> candleMeta = new ConcurrentHashMap<>();
    private final AtomicLong lastRecoveryTriggerMs = new AtomicLong(0);
    private static final long RECOVERY_DEBOUNCE_MS = 5_000;

    public MarketDataManager(MarketDataStreamManager streamManager) {
        this.streamManager = streamManager;

        ServiceStubFactory serviceFactory = streamManager.getStreamFactory().getServiceStubFactory();
        this.marketDataService = serviceFactory.newResilienceAsyncService(
                MarketDataServiceGrpc::newStub,
                ResilienceConfiguration.builder(executorService, serviceFactory.getConfiguration()).build()
        );

        streamManager.addOnConnectListener(() -> {
            if (firstConnectSeen.compareAndSet(false, true)) {
                return;
            }
            var now = System.currentTimeMillis();
            if (now - lastRecoveryTriggerMs.getAndSet(now) < RECOVERY_DEBOUNCE_MS) {
                return;
            }
            recoverMissedCandles();
        });
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

    /**
     * Метод для подписки на свечи по списку инструментов.
     * <p>
     * При получении новых свечей:
     * <ul>
     *   <li>Если recovery не выполняется — свеча сразу передаётся пользователю</li>
     *   <li>Если recovery выполняется — свеча временно буферизуется</li>
     * </ul>
     *
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
        instruments.forEach(instrument -> {
            var key = CandleKey.of(instrument, spec);
            replay.put(key, onCandleListener);
            pending.putIfAbsent(key, new ConcurrentSkipListMap<>());
        });

        streamManager.subscribeCandles(instruments, spec, candle -> {
            var key = CandleKey.of(candle.getInstrumentUid(), candle.getInterval(), spec);
            var original = candle.getOriginal();
            candleMeta.putIfAbsent(key, new CandleMeta(
                    original.getFigi(),
                    original.getTicker(),
                    original.getClassCode(),
                    original.getCandleSourceType()
            ));

            var gate = recoveryGate.get();
            if (!gate.isDone()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Waiting for recovery gate {}, with time {}",
                            candle.getInstrumentUid(),
                            candle.getTime());
                }
                pending.computeIfAbsent(key, k -> new ConcurrentSkipListMap<>())
                        .put(candle.getTime(), new CandleWrapper(candle.getOriginal(), true));
                return;
            }

            onCandleListener.onNext(candle);

            lastTime.merge(key, candle.getTime(), (oldV, newV) -> newV.isAfter(oldV) ? newV : oldV);
        });
    }

    /**
     * Метод для отписки от свечей по списку инструментов.
     * <p>
     * Удаляет внутренние структуры:
     * <ul>
     *   <li>листенер</li>
     *   <li>последнее полученное время свечи</li>
     *   <li>буфер ожидающих свечей</li>
     * </ul>
     *
     * @param instruments список инструментов {@link Instrument}
     * @param spec        параметры подписки {@link CandleSubscriptionSpec}
     */
    public void unsubscribeCandles(Set<Instrument> instruments, CandleSubscriptionSpec spec) {
        streamManager.unsubscribeCandles(instruments, spec);
        instruments.forEach(i -> {
            var key = CandleKey.of(i, spec);
            replay.remove(key);
            lastTime.remove(key);
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
    private void recoverMissedCandles() {
        if (!recoveryInProgress.compareAndSet(false, true)) {
            return;
        }
        var gate = new CompletableFuture<Void>();
        recoveryGate.set(gate);
        logger.info("Starting candles recovery...");
        var futures = replay.entrySet().stream()
                .map(e -> recoverOne(e.getKey(), e.getValue()))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures)
                .whenCompleteAsync((unused, throwable) -> {
                    try {
                        if (throwable != null) {
                            logger.warn("Candles recovery failed", throwable);
                        } else {
                            logger.info("Candles recovery finished");
                        }
                        flushAllPending();
                    } finally {
                        recoveryInProgress.set(false);
                        gate.complete(null);
                    }
                }, executorService);
    }

    /**
     * Восстанавливает пропущенные свечи для конкретной подписки.
     * <p>
     *
     * @param key      ключ подписки
     * @param listener пользовательский обработчик
     * @return future завершения восстановления
     */
    private CompletableFuture<Void> recoverOne(CandleKey key, OnNextListener<CandleWrapper> listener) {
        var time = lastTime.get(key);
        var endTime = Instant.now().truncatedTo(ChronoUnit.MINUTES)
                .atOffset(ZoneOffset.UTC)
                .toLocalDateTime();
        if (time == null) {
            return CompletableFuture.runAsync(() -> flushPendingAll(key, listener), executorService);
        }
        var request = GetCandlesRequest.newBuilder()
                .setFrom(TimeMapper.localDateTimeToTimestamp(time))
                .setTo(TimeMapper.localDateTimeToTimestamp(endTime))
                .setInstrumentId(key.instrumentId)
                .setInterval(CandleInterval.forNumber(key.interval.getNumber()))
                .setCandleSourceType(key.candleSource)
                .build();

        var responseFuture = marketDataService.callAsyncMethod(
                MarketDataServiceGrpc.getGetCandlesMethod(),
                (stub, obs) -> stub.getCandles(request, obs)
        );

        return responseFuture
                .thenApplyAsync(response -> response.getCandlesList().stream()
                        .filter(HistoricCandle::getIsComplete)
                        .map(h -> toHistoryWrapper(h, key))
                        .filter(cw -> cw.getTime().isAfter(time))
                        .sorted(Comparator.comparing(CandleWrapper::getTime))
                        .toList(), executorService)
                .thenAcceptAsync(history -> {
                    for (var cw : history) {
                        listener.onNext(cw);
                        lastTime.put(key, cw.getTime());
                    }

                    var map = pending.get(key);
                    if (map == null || map.isEmpty()) {
                        return;
                    }

                    var last = lastTime.get(key);
                    if (last == null) {
                        flushPendingAll(key, listener);
                        return;
                    }

                    var tail = map.tailMap(last.plusNanos(1), true);
                    for (var entry : tail.entrySet()) {
                        var cw = entry.getValue();
                        listener.onNext(cw);
                        lastTime.put(key, cw.getTime());
                    }

                    var newLast = lastTime.get(key);
                    map.headMap(newLast.plusNanos(1), false).clear();
                }, executorService);
    }

    /**
     * Сбрасывает буфер всех подписок после завершения recovery.
     * <p>
     */
    private void flushAllPending() {
        for (var entry : pending.entrySet()) {
            var key = entry.getKey();
            var map = entry.getValue();
            if (map == null || map.isEmpty()) continue;

            var listener = replay.get(key);
            if (listener == null) {
                map.clear();
                continue;
            }

            flushMapPending(key, listener, map);
        }
    }

    /**
     * Сбрасывает буфер свечей для конкретной подписки.
     *
     * @param key      ключ подписки
     * @param listener пользовательский обработчик
     */
    private void flushPendingAll(CandleKey key, OnNextListener<CandleWrapper> listener) {
        var map = pending.get(key);
        if (map == null || map.isEmpty()) return;

        flushMapPending(key, listener, map);
    }

    /**
     * Доставляет пользователю все буферизованные свечи
     * в порядке их времени и обновляет lastTime.
     *
     * @param key      ключ подписки
     * @param listener пользовательский обработчик
     * @param map      буфер свечей
     */
    private void flushMapPending(CandleKey key, OnNextListener<CandleWrapper> listener, NavigableMap<LocalDateTime, CandleWrapper> map) {
        for (var e : map.entrySet()) {
            var cw = e.getValue();
            if (logger.isDebugEnabled()) {
                logger.debug("Flushing pending candle {}, with time{}", key.instrumentId, cw.getTime());
            }
            listener.onNext(cw);
            lastTime.merge(key, cw.getTime(), (oldV, newV) -> newV.isAfter(oldV) ? newV : oldV);
        }
        map.clear();
    }

    private CandleWrapper toHistoryWrapper(HistoricCandle c, CandleKey key) {
        var meta = candleMeta.get(key);
        var candle = Candle.newBuilder()
                .setInstrumentUid(key.instrumentId)
                .setInterval(key.interval)
                .setTime(c.getTime())
                .setOpen(c.getOpen())
                .setHigh(c.getHigh())
                .setLow(c.getLow())
                .setClose(c.getClose())
                .setVolume(c.getVolume())
                .setVolumeBuy(c.getVolumeBuy())
                .setVolumeSell(c.getVolumeSell());

        if (meta != null) {
            var candleSource = meta.candleSource() != null ? meta.candleSource() : CandleSource.CANDLE_SOURCE_UNSPECIFIED;
            candle.setCandleSourceType(candleSource);
            setIfPresent(meta.figi(), candle::setFigi);
            setIfPresent(meta.ticker(), candle::setTicker);
            setIfPresent(meta.classCode(), candle::setClassCode);
        }

        return new CandleWrapper(candle.build(), true);
    }

    private record CandleKey(
            String instrumentId,
            SubscriptionInterval interval,
            GetCandlesRequest.CandleSource candleSource,
            boolean waitingClose
    ) {
        static CandleKey of(Instrument i, CandleSubscriptionSpec spec) {
            return new CandleKey(
                    i.getInstrumentId().id(),
                    i.getSubscriptionInterval(),
                    spec.getCandleSource(),
                    spec.isWaitingClose()
            );
        }

        static CandleKey of(String uid, SubscriptionInterval interval, CandleSubscriptionSpec spec) {
            return new CandleKey(uid, interval, spec.getCandleSource(), spec.isWaitingClose());
        }
    }

    private record CandleMeta(String figi, String ticker, String classCode, CandleSource candleSource) {
    }

    private static void setIfPresent(String value, Consumer<String> setter) {
        if (value != null && !value.isBlank()) {
            setter.accept(value);
        }
    }
}
