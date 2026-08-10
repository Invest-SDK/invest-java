package ru.ttech.piapi.core.impl.marketdata;

import io.vavr.Lazy;
import io.vavr.collection.List;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.tinkoff.piapi.contract.v1.MarketDataRequest;
import ru.tinkoff.piapi.contract.v1.MarketDataResponse;
import ru.tinkoff.piapi.contract.v1.MarketDataStreamServiceGrpc;
import ru.tinkoff.piapi.contract.v1.PingDelaySettings;
import ru.ttech.piapi.core.connector.streaming.BidirectionalStreamWrapper;
import ru.ttech.piapi.core.connector.streaming.StreamServiceStubFactory;
import ru.ttech.piapi.core.connector.streaming.listeners.OnConnectListener;
import ru.ttech.piapi.core.impl.wrapper.OnConnectEvent;
import ru.ttech.piapi.core.impl.marketdata.subscription.Instrument;
import ru.ttech.piapi.core.impl.marketdata.subscription.MarketDataSubscriptionResult;
import ru.ttech.piapi.core.impl.marketdata.subscription.RequestAction;
import ru.ttech.piapi.core.impl.marketdata.subscription.SubscriptionEntry;
import ru.ttech.piapi.core.impl.marketdata.subscription.SubscriptionStatus;
import ru.ttech.piapi.core.impl.marketdata.util.MarketDataRequestUtil;
import ru.ttech.piapi.core.impl.marketdata.util.MarketDataResponseUtil;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Resilience-обёртка над {@link BidirectionalStreamWrapper}.
 * Переподключает стрим при разрыве соединения (превышении inactivity-timeout) и повторно подписывается на обновления
 */
public class MarketDataStreamWrapper {

  protected static final Logger logger = LoggerFactory.getLogger(MarketDataStreamWrapper.class);

  @Getter
  private final UUID uuid = UUID.randomUUID();
  protected final AtomicLong lastInteractionTime = new AtomicLong();
  protected final LinkedBlockingQueue<MarketDataSubscriptionResult> subscriptionResultsQueue = new LinkedBlockingQueue<>();
  protected final AtomicInteger subscriptionsCount = new AtomicInteger(0);
  protected final Map<MarketDataResponseType, Set<SubscriptionEntry>> subscriptionsMap = Map.of(
    MarketDataResponseType.CANDLE, ConcurrentHashMap.newKeySet(),
    MarketDataResponseType.LAST_PRICE, ConcurrentHashMap.newKeySet(),
    MarketDataResponseType.TRADE, ConcurrentHashMap.newKeySet(),
    MarketDataResponseType.ORDER_BOOK, ConcurrentHashMap.newKeySet(),
    MarketDataResponseType.TRADING_STATUS, ConcurrentHashMap.newKeySet()
  );
  protected final AtomicReference<List<MarketDataRequest>> requestsRef = new AtomicReference<>(List.empty());
  protected final AtomicReference<List<MarketDataRequest>> pendingRetryRequestsRef = new AtomicReference<>(List.empty());
  protected final Map<MarketDataRequest, Integer> resubscriptionAttempts = new ConcurrentHashMap<>();
  protected final AtomicReference<ScheduledFuture<?>> healthCheckFutureRef = new AtomicReference<>(null);
  protected final AtomicReference<CompletableFuture<MarketDataSubscriptionResult>> lastTask = new AtomicReference<>();
  protected final AtomicBoolean isResubscribing = new AtomicBoolean(false);
  protected final int pingDelay;
  protected final int inactivityTimeout;
  protected final int maxResubscriptionAttempts;
  protected final BidirectionalStreamWrapper<
    MarketDataStreamServiceGrpc.MarketDataStreamServiceStub,
    MarketDataRequest,
    MarketDataResponse> streamWrapper;
  protected final ScheduledExecutorService executorService;
  protected final List<OnConnectListener<OnConnectEvent>> onConnectListeners;
  protected final List<OnConnectListener<OnConnectEvent>> onConnectListenersBlockInstruments;
  protected final AtomicBoolean needToRecovery = new AtomicBoolean(false);
  protected final AtomicBoolean needToUpdateReceiveTime = new AtomicBoolean(true);

  public MarketDataStreamWrapper(
    StreamServiceStubFactory streamFactory,
    MarketDataStreamWrapperConfiguration configuration
  ) {
    this.streamWrapper = streamFactory.newBidirectionalStream(configuration.getStreamWrapperConfigBuilder()
      .addOnNextListener(this::processResponse)
      .build());
    this.onConnectListeners = List.ofAll(configuration.getOnConnectListeners());
    this.onConnectListenersBlockInstruments = List.ofAll(configuration.getOnConnectListenersBlockInstruments());
    this.executorService = configuration.getExecutorService();
    this.inactivityTimeout = streamFactory.getServiceStubFactory().getConfiguration().getStreamInactivityTimeout();
    this.pingDelay = streamFactory.getServiceStubFactory().getConfiguration().getStreamPingDelay();
    this.maxResubscriptionAttempts =
      streamFactory.getServiceStubFactory().getConfiguration().getMaxMarketDataResubscriptionAttempts();
    this.lastTask.set(CompletableFuture.completedFuture(null));
  }

  /**
   * Останавливает healthcheck, очищает очередь запросов и отключается от стрима
   */
  public void disconnect() {
    if (healthCheckFutureRef.get() != null) {
      healthCheckFutureRef.get().cancel(true);
      healthCheckFutureRef.set(null);
    }
    requestsRef.updateAndGet(requests -> List.empty());
    pendingRetryRequestsRef.set(List.empty());
    resubscriptionAttempts.clear();
    subscriptionsMap.values().forEach(Set::clear);
    updateSubscriptionsCount();
    disconnectWrapper();
  }

  protected void disconnectWrapper() {
    streamWrapper.disconnect();
  }

  protected void sendPingSettings() {
    var pingRequest = MarketDataRequest.newBuilder()
      .setPingSettings(PingDelaySettings.newBuilder()
        .setPingDelayMs(pingDelay)
        .build())
      .build();
    streamWrapper.newCall(pingRequest);
  }

  /**
   * Метод для отправки запроса в стрим. Можно подписаться или отписаться от обновлений
   *
   * @param request запрос на подписку или отписку
   * @return результат выполнения запроса
   */
  public CompletableFuture<MarketDataSubscriptionResult> newCall(MarketDataRequest request) {
    var supplier = Lazy.of(() -> CompletableFuture.supplyAsync(() -> {
      var requestType = MarketDataRequestUtil.determineRequestType(request);
      subscriptionResultsQueue.clear();
      if (!streamWrapper.isConnected()) {
        streamWrapper.connect();
      }
      streamWrapper.newCall(request);
      lastInteractionTime.set(System.currentTimeMillis());
      if (!isResubscribing.get()) {
        requestsRef.updateAndGet(requests -> requests.append(request));
      }
      return waitSubscriptionResult(requestType, MarketDataRequestUtil.extractInstruments(request));
    }));
    return lastTask.updateAndGet(previousTask ->
      previousTask
        .thenCompose(previousResult -> supplier.get())
        .exceptionally(ex -> null)
    );
  }

  protected MarketDataSubscriptionResult waitSubscriptionResult(
    MarketDataResponseType responseType,
    Set<Instrument> instruments
  ) {
    long deadline = System.currentTimeMillis() + inactivityTimeout;
    try {
      while (true) {
        long remainingTime = deadline - System.currentTimeMillis();
        if (remainingTime <= 0) {
          logger.error("Unexpected subscription result in wrapper {}: null", uuid);
          throw new IllegalStateException("Wrong subscription result!");
        }
        var subscriptionResult = subscriptionResultsQueue.poll(remainingTime, TimeUnit.MILLISECONDS);
        if (subscriptionResult == null) {
          logger.error("Unexpected subscription result in wrapper {}: null", uuid);
          throw new IllegalStateException("Wrong subscription result!");
        }
        if (subscriptionResult.getResponseType() == responseType
          && checkSubscribedInstruments(subscriptionResult.getSubscriptionStatusMap().keySet(), instruments)
        ) {
          return subscriptionResult;
        }
        logger.debug("Discarding stale subscription result in wrapper {}: {}", uuid, subscriptionResult);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for subscription response", e);
    }
  }

  protected void processResponse(MarketDataResponse response) {
    lastInteractionTime.set(System.currentTimeMillis());
    if (needToUpdateReceiveTime.get()) {
    MarketDataResponseUtil.getResponseType(response).ifPresent(responseType ->
      MarketDataResponseUtil.extractInstrument(response).ifPresent(instrument -> {
        var entries = subscriptionsMap.get(responseType);
        if (entries != null) {
          entries.stream()
            .filter(entry -> entry.subscriptionEquals(instrument))
            .findFirst()
            .ifPresent(entry -> entry.updateLastUpdateTime(Instant.now()
            .atOffset(ZoneOffset.UTC)
            .toLocalDateTime().truncatedTo(ChronoUnit.SECONDS)));
        }
      })
    );
    }

    MarketDataResponseUtil.getSubscriptionResultFromResponse(response)
      .ifPresent(this::processSubscriptionResult);
  }

  protected void processSubscriptionResult(MarketDataSubscriptionResult subscriptionResult) {
    try {
      updateSubscriptionsMap(subscriptionResult);
      if (subscriptionsCount.get() > 0) {
        sendPingSettings();
        if (healthCheckFutureRef.get() == null) {
          logger.debug("Healthcheck enabled in wrapper {}", uuid);
          //onConnectListeners.forEach(Runnable::run);
          healthCheckFutureRef.set(executorService.scheduleAtFixedRate(this::healthCheck, 0, pingDelay, TimeUnit.MILLISECONDS));
        }
      } else if (!isResubscribing.get() && healthCheckFutureRef.get() != null) {
        logger.info("Healthcheck disabled in wrapper {}", uuid);
        healthCheckFutureRef.get().cancel(true);
        healthCheckFutureRef.set(null);
      }
      subscriptionResultsQueue.put(subscriptionResult);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Метод для проверки статуса подписки на определённый тип обновлений по инструменту
   *
   * @param responseType Тип обновлений
   * @param instrument   Инструмент
   * @return true, если подписка есть, false - если нет
   */
  public boolean isSubscribed(MarketDataResponseType responseType, Instrument instrument) {
    return getSubscribedEntries(responseType).stream()
      .anyMatch(entry -> entry.subscriptionEquals(instrument));
  }

  protected Set<SubscriptionEntry> getSubscribedEntries(MarketDataResponseType responseType) {
    if (!subscriptionsMap.containsKey(responseType)) {
      throw new IllegalArgumentException("Unsupported response type: " + responseType);
    }
    return subscriptionsMap.get(responseType);
  }

  /**
   * Метод актуализации подписок враппера
   *
   * @param subscriptionResult - результат подписки, полученный от сервиса
   */
  protected void updateSubscriptionsMap(MarketDataSubscriptionResult subscriptionResult) {
    var currentAction = subscriptionResult.getSubscriptionAction();
    var subscribedInstruments = getSubscribedEntries(subscriptionResult.getResponseType());
    var errorResults = subscriptionResult.getSubscriptionStatusMap().entrySet().stream()
      .filter(entry ->  entry.getValue().isError())
      .map(Map.Entry::getKey)
      .collect(Collectors.toSet());
    var successResults = subscriptionResult.getSubscriptionStatusMap().entrySet().stream()
      .filter(entry -> entry.getValue().isOk())
      .peek(entry -> {
        if (currentAction == RequestAction.SUBSCRIBE) {
          logger.debug("Success subscribed in wrapper {} for instrument: {}", uuid, entry.getKey().getInstrumentId());
        } else if (currentAction == RequestAction.UNSUBSCRIBE) {
          logger.debug("Success unsubscribed in wrapper {} for instrument: {}", uuid, entry.getKey().getInstrumentId());
        }
      })
      .map(Map.Entry::getKey)
      .collect(Collectors.toSet());
    if (currentAction == RequestAction.SUBSCRIBE) {
      successResults.forEach(instr -> {
        SubscriptionEntry probe = new SubscriptionEntry(instr);

        if (!subscribedInstruments.add(probe)) {
          setInstrumentActive(instr, subscribedInstruments);
        }
      });

    } else if (currentAction == RequestAction.UNSUBSCRIBE) {
      successResults.forEach(instr ->
        subscribedInstruments.removeIf(entry -> entry.subscriptionEquals(instr))
      );

    }
    errorResults.forEach(instr ->
      subscribedInstruments.removeIf(entry -> entry.subscriptionEquals(instr))
    );
    updateSubscriptionsCount();
  }

  private static void setInstrumentActive(Instrument instr, Set<SubscriptionEntry> subscribedInstruments) {
    subscribedInstruments.stream()
      .filter(entry -> entry.subscriptionEquals(instr))
      .findFirst()
      .ifPresent(entry -> entry.setActive(true));
  }

  /**
   * Метод актуализации количества подписок.
   * Обновляет количество подписок враппера исходя из текущего состояния subscriptionsMap
   */
  protected void updateSubscriptionsCount() {
    int currentSize = subscriptionsMap.values().stream().mapToInt(Set::size).sum();
    subscriptionsCount.set(currentSize);
  }

  protected void healthCheck() {
    long currentTime = System.currentTimeMillis();
    boolean timeoutExceeded = currentTime - lastInteractionTime.get() > inactivityTimeout;
    if (timeoutExceeded && subscriptionsCount.get() > 0) {
      logger.info("Wrapper {} reconnecting...", uuid);
      needToUpdateReceiveTime.set(false);
      onConnectListenersBlockInstruments.forEach(
        onConnectListener -> {
          var action = OnConnectEvent.builder()
            .id(uuid)
            .build();
          onConnectListener.onConnect(action);
        });
      needToRecovery.set(true);
      disconnectWrapper();
      isResubscribing.set(true);
      setSubscriptionsNonActive();
      resubscriptionAttempts.clear();
      pendingRetryRequestsRef.set(attemptResubscription(requestsRef.get()));
      isResubscribing.set(false);
      lastInteractionTime.set(System.currentTimeMillis());
    }
    if (needToRecovery.get()){
      var subscriptionsActiveCount = subscriptionsActiveCount();
      if (subscriptionsCount.get() > 0 && subscriptionsActiveCount == subscriptionsCount.get()) {
        needToRecovery.set(false);
        pendingRetryRequestsRef.set(List.empty());
        resubscriptionAttempts.clear();
        onConnectListeners.forEach(
          onConnectListener -> {
            var action = OnConnectEvent.builder()
              .id(uuid)
              .build();
            onConnectListener.onConnect(action);
          });
      } else if (!timeoutExceeded && !pendingRetryRequestsRef.get().isEmpty()) {
        logger.info("Retrying {} unconfirmed subscription request(s) in wrapper {}",
          pendingRetryRequestsRef.get().size(), uuid);
        isResubscribing.set(true);
        pendingRetryRequestsRef.set(attemptResubscription(pendingRetryRequestsRef.get()));
        isResubscribing.set(false);
      }
    }
  }

  /**
   * Повторно отправляет каждый запрос из списка, пока не будет получено подтверждение. Лимит попыток
   * ({@code maxResubscriptionAttempts}) расходуется только на явные отказы сервера (статус ошибки подписки
   * по инструменту) - таймауты и прочие исключения (например, проблемы с DNS) лимит не тратят
   * и запрос продолжает повторяться на следующих тиках healthCheck без ограничения по количеству
   *
   * @param requests запросы на подписку, ожидающие подтверждения
   * @return запросы, которые всё ещё не подтверждены и не исчерпали лимит попыток
   */
  private List<MarketDataRequest> attemptResubscription(List<MarketDataRequest> requests) {
    return requests.filter(request -> {
      var result = newCall(request).join();
      if (result == null) {
        return true;
      }
      boolean hasRejection = result.getSubscriptionStatusMap().values().stream()
        .anyMatch(SubscriptionStatus::isError);
      if (!hasRejection) {
        resubscriptionAttempts.remove(request);
        return false;
      }
      int attempt = resubscriptionAttempts.merge(request, 1, Integer::sum);
      if (attempt >= maxResubscriptionAttempts) {
        logger.error(
          "Giving up resubscription in wrapper {} after {} rejected attempt(s) for request: {}",
          uuid, attempt, request);
        resubscriptionAttempts.remove(request);
        return false;
      }
      return true;
    });
  }

  private long subscriptionsActiveCount() {
    return subscriptionsMap.values().stream().flatMap(Set::stream).filter(SubscriptionEntry::isActive).count();
  }

  private void setSubscriptionsNonActive() {
    subscriptionsMap.values().forEach(set ->
      set.forEach(value -> value.setActive(false)));
  }

  private boolean checkSubscribedInstruments(
    Set<Instrument> subscribedInstruments,
    Set<Instrument> requestedInstruments
  ) {
    if (subscribedInstruments.size() != requestedInstruments.size()) {
      return false;
    }
    return requestedInstruments.stream()
      .allMatch(req -> subscribedInstruments.stream().anyMatch(req::subscriptionEquals));
  }

  protected void updateLastReceiveTime(Instrument instrument, LocalDateTime lastReceiveTime) {
    subscriptionsMap.values().stream()
      .flatMap(Set::stream)
      .filter(entry -> entry.subscriptionEquals(instrument))
      .findFirst()
      .ifPresent(entry -> entry.updateLastUpdateTime(lastReceiveTime));
  }

  protected void setNeedToUpdateReceiveTime(boolean updateTime) {
    needToUpdateReceiveTime.set(updateTime);
  }

  public int getSubscriptionsCount() {
    return subscriptionsCount.get();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MarketDataStreamWrapper that = (MarketDataStreamWrapper) o;
    return uuid.equals(that.uuid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uuid);
  }
}
