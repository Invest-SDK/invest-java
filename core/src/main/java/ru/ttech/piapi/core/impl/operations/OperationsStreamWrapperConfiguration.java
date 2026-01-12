package ru.ttech.piapi.core.impl.operations;

import ru.tinkoff.piapi.contract.v1.OperationsStreamRequest;
import ru.tinkoff.piapi.contract.v1.OperationsStreamResponse;
import ru.tinkoff.piapi.contract.v1.OperationsStreamServiceGrpc;
import ru.tinkoff.piapi.contract.v1.PingDelaySettings;
import ru.ttech.piapi.core.connector.resilience.ResilienceServerSideStreamWrapper;
import ru.ttech.piapi.core.connector.resilience.ResilienceServerSideStreamWrapperConfiguration;
import ru.ttech.piapi.core.connector.streaming.ServerSideStreamConfiguration;
import ru.ttech.piapi.core.connector.streaming.listeners.OnNextListener;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Конфигурация для обёртки {@link ResilienceServerSideStreamWrapper} над стримом операций сервиса {@link OperationsStreamServiceGrpc}
 */
public class OperationsStreamWrapperConfiguration extends ResilienceServerSideStreamWrapperConfiguration<OperationsStreamRequest, OperationsStreamResponse> {


  protected OperationsStreamWrapperConfiguration(
    ScheduledExecutorService executorService,
    List<OnNextListener<OperationsStreamResponse>> onResponseListeners,
    List<Runnable> onConnectListeners
  ) {
    super(executorService, onResponseListeners, onConnectListeners);
  }

  @Override
  public Function<OperationsStreamRequest,
    ServerSideStreamConfiguration.Builder<?, OperationsStreamRequest, OperationsStreamResponse>> getConfigurationBuilder(int pingDelay) {
    return request -> {
      var requestWithPing = OperationsStreamRequest.newBuilder(request)
        .setPingSettings(PingDelaySettings.newBuilder().setPingDelayMs(pingDelay).build())
        .build();
      return ServerSideStreamConfiguration.builder(
          OperationsStreamServiceGrpc::newStub,
          OperationsStreamServiceGrpc.getOperationsStreamMethod(),
          (stub, observer) -> stub.operationsStream(requestWithPing, observer))
        .addOnNextListener(response -> {
          if (response.hasOperation()) {
            onResponseListeners.forEach(listener -> listener.onNext(response));
          }
        });
    };
  }

  @Override
  public BiFunction<OperationsStreamRequest, OperationsStreamResponse, Optional<OperationsStreamRequest>> getSubscriptionResultProcessor() {
    return (request, response) -> {
      if (response.hasSubscriptions()) {
        var successSubscriptionsList = response.getSubscriptions().getAccountsList();
        if (!successSubscriptionsList.isEmpty()) {
          return Optional.of(request);
        }
      }
      return Optional.empty();
    };
  }

  /**
   * Фабричный метод получения нового билдера для создания конфигурации.
   * <p>Пример вызова:<pre>{@code
   *     var operationsConfig = OperationsStreamWrapperConfiguration.builder(executorService)
   *       .addOnResponseListener(operation -> logger.info("New operation: {}", operation))
   *       .addOnConnectListener(() -> logger.info("Stream connected!"))
   *       .build();
   * }</pre>
   * @param executorService планировщик для проверки состояния соединения
   * @return Билдер конфигурации обёртки над стримом
   */
  public static OperationsStreamWrapperConfiguration.Builder builder(ScheduledExecutorService executorService) {
    return new OperationsStreamWrapperConfiguration.Builder(executorService);
  }

  public static class Builder extends ResilienceServerSideStreamWrapperConfiguration.Builder<
    OperationsStreamRequest, OperationsStreamResponse> {

    protected Builder(ScheduledExecutorService executorService) {
      super(executorService);
    }

    @Override
    public OperationsStreamWrapperConfiguration build() {
      return new OperationsStreamWrapperConfiguration(executorService, onResponseListeners, onConnectListeners);
    }
  }
}
