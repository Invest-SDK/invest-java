package ru.ttech.piapi.core.connector.stream;

import io.grpc.Status;
import io.grpc.StatusException;
import io.sentry.Sentry;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.grpcmock.junit5.GrpcMockExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.tinkoff.piapi.contract.v1.OrderStateStreamRequest;
import ru.tinkoff.piapi.contract.v1.OrderStateStreamResponse;
import ru.tinkoff.piapi.contract.v1.OrdersStreamServiceGrpc;
import ru.tinkoff.piapi.contract.v1.SubscriptionResponse;
import ru.ttech.piapi.core.connector.GrpcStubBaseTest;
import ru.ttech.piapi.core.connector.errorhub.ErrorHubConfigurator;
import ru.ttech.piapi.core.connector.streaming.ServerSideStreamConfiguration;
import ru.ttech.piapi.core.connector.streaming.StreamServiceStubFactory;

import java.util.concurrent.CountDownLatch;

import static org.grpcmock.GrpcMock.response;
import static org.grpcmock.GrpcMock.serverStreamingMethod;
import static org.grpcmock.GrpcMock.statusException;
import static org.grpcmock.GrpcMock.stream;
import static org.grpcmock.GrpcMock.stubFor;
import static org.grpcmock.GrpcMock.times;
import static org.grpcmock.GrpcMock.verifyThat;

@Slf4j
@ExtendWith(GrpcMockExtension.class)
public class ErrorHubTest extends GrpcStubBaseTest {

  void initWithLatch(CountDownLatch latch) {
    Sentry.init(options -> {
        ErrorHubConfigurator.configure(options);
        options.setDebug(true);
        options.setEnvironment("test");
        var beforeSend = options.getBeforeSend();
        options.setBeforeSend(((sentryEvent, hint) -> {
          var event = beforeSend.execute(sentryEvent, hint);
          if (event != null) {
            latch.countDown();
          }
          return null;
        }));
      }
    );
  }

  @Test
  void testBlacklist() {
    var latch = new CountDownLatch(1);
    try {
      initWithLatch(latch);
      throw new Exception("This is blacklisted exception");
    } catch (Exception e) {
      Sentry.captureException(e);
      Assertions.assertEquals(1, latch.getCount());
    }
  }

  @Test
  void testWhitelist() {
    var latch = new CountDownLatch(1);
    try {
      initWithLatch(latch);
      throw new StatusException(Status.INTERNAL);
    } catch (Exception e) {
      Sentry.captureException(e);
      Assertions.assertEquals(0, latch.getCount());
    }
  }

  @SneakyThrows
  @Test
  public void serverSideStream_internal() {
    // setup mock stub
    var massageLatch = new CountDownLatch(2);
    var errorHubLatch = new CountDownLatch(1);
    var response = OrderStateStreamResponse.newBuilder().setSubscription(SubscriptionResponse.newBuilder().addAccounts("accc").build()).build();
    stubFor(serverStreamingMethod(OrdersStreamServiceGrpc.getOrderStateStreamMethod())
      .willReturn(
        stream(response(response).withFixedDelay(1000))
          .and(statusException(Status.INTERNAL))
      ));

    // setup server-side stream
    var factoryWithConfig = createStubFactory();
    var factory = factoryWithConfig._1();
    var streamFactory = StreamServiceStubFactory.create(factory);
    var stream = streamFactory.newServerSideStream(
      ServerSideStreamConfiguration.builder(
          OrdersStreamServiceGrpc::newStub,
          OrdersStreamServiceGrpc.getOrderStateStreamMethod(),
          (stub, observer) -> stub.orderStateStream(OrderStateStreamRequest.getDefaultInstance(), observer))
        .addOnNextListener(markerDataResponse -> massageLatch.countDown())
        .addOnErrorListener(throwable -> massageLatch.countDown())
        .build()
    );
    stream.connect();
    initWithLatch(errorHubLatch);

    massageLatch.await();
    stream.disconnect();
    Assertions.assertEquals(0, errorHubLatch.getCount());
    verifyThat(OrdersStreamServiceGrpc.getOrderStateStreamMethod(), times(1));
  }

  @SneakyThrows
  @Test
  public void serverSideStream_invalid_argument() {
    // setup mock stub
    var massageLatch = new CountDownLatch(2);
    var errorHubLatch = new CountDownLatch(1);
    var response = OrderStateStreamResponse.newBuilder().setSubscription(SubscriptionResponse.newBuilder().addAccounts("accc").build()).build();
    stubFor(serverStreamingMethod(OrdersStreamServiceGrpc.getOrderStateStreamMethod())
      .willReturn(
        stream(response(response).withFixedDelay(1000))
          .and(statusException(Status.INVALID_ARGUMENT))
      ));

    // setup server-side stream
    var factoryWithConfig = createStubFactory();
    var factory = factoryWithConfig._1();
    var streamFactory = StreamServiceStubFactory.create(factory);
    var stream = streamFactory.newServerSideStream(
      ServerSideStreamConfiguration.builder(
          OrdersStreamServiceGrpc::newStub,
          OrdersStreamServiceGrpc.getOrderStateStreamMethod(),
          (stub, observer) -> stub.orderStateStream(OrderStateStreamRequest.getDefaultInstance(), observer))
        .addOnNextListener(markerDataResponse -> massageLatch.countDown())
        .addOnErrorListener(throwable -> massageLatch.countDown())
        .build()
    );
    stream.connect();
    initWithLatch(errorHubLatch);

    massageLatch.await();
    stream.disconnect();
    Assertions.assertEquals(1, errorHubLatch.getCount());
    verifyThat(OrdersStreamServiceGrpc.getOrderStateStreamMethod(), times(1));
  }

}
