package ru.ttech.piapi.core.connector.internal;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.sentry.Sentry;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.Set;

@Slf4j
@SuppressWarnings("unchecked")
public class ErrorHubReportInterceptor implements ClientInterceptor {

  private static final Set<Status.Code> IGNORED_STATUS_CODES = Set.of(
    Status.Code.OK,
    Status.Code.CANCELLED,
    Status.Code.INVALID_ARGUMENT,
    Status.Code.NOT_FOUND,
    Status.Code.RESOURCE_EXHAUSTED,
    Status.Code.FAILED_PRECONDITION
  );
  private final String targetUrl;

  public ErrorHubReportInterceptor(String targetUrl) {
    this.targetUrl = targetUrl;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
    MethodDescriptor<ReqT, RespT> method,
    CallOptions callOptions,
    Channel next
  ) {
    return new ErrorReportClientCall<>(
      next.newCall(method, callOptions),
      method,
      targetUrl
    );
  }

  static class ErrorReportClientCall<ReqT, RespT>
    extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {

    private final MethodDescriptor<ReqT, RespT> method;
    private final String targetUrl;

    ErrorReportClientCall(
      ClientCall<ReqT, RespT> call,
      MethodDescriptor<ReqT, RespT> method,
      String targetUrl
    ) {
      super(call);
      this.method = method;
      this.targetUrl = targetUrl;
    }

    @Override
    public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {
      if (method.getType() != MethodDescriptor.MethodType.UNARY) {
        super.start(new ErrorHubReportClientCallListener<>(responseListener, method, targetUrl), headers);
      } else {
        super.start(responseListener, headers);
      }
    }
  }

  static class ErrorHubReportClientCallListener<RespT>
    extends ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT> {

    private static final Metadata.Key<String> trackingIdKey =
      Metadata.Key.of("x-tracking-id", Metadata.ASCII_STRING_MARSHALLER);
    public static final DateTimeFormatter ISO_DATE_TIME_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

    private final MethodDescriptor<?, RespT> method;
    private final LocalDateTime startTime;
    private final String targetUrl;
    private RespT lastSuccessfulMessage;
    private String trackingId;

    ErrorHubReportClientCallListener(
      ClientCall.Listener<RespT> listener,
      MethodDescriptor<?, RespT> method, String targetUrl
    ) {
      super(listener);
      this.method = method;
      this.targetUrl = targetUrl;
      this.startTime = LocalDateTime.now();
    }

    @Override
    public void onHeaders(Metadata headers) {
      trackingId = headers.get(trackingIdKey);
      delegate().onHeaders(headers);
    }

    @Override
    public void onMessage(RespT message) {
      lastSuccessfulMessage = message;
      delegate().onMessage(message);
    }

    @Override
    public void onClose(Status status, Metadata trailers) {
      if (!IGNORED_STATUS_CODES.contains(status.getCode())) {
        reportErrorHub(status.asException());
      }
      delegate().onClose(status, trailers);
    }

    private void reportErrorHub(Exception exception) {
      var connectionDuration = Duration.between(startTime.atZone(ZoneId.systemDefault()).toInstant(), Instant.now());
      var durationString = String.format("%.3f sec.", (double) connectionDuration.toMillis() / 1000);
      Sentry.captureException(exception, scope -> {
        scope.setExtra("tracking_id", trackingId);
        scope.setExtra("method_name", method.getFullMethodName());
        scope.setExtra("stream_start_time", ISO_DATE_TIME_FORMATTER.format(startTime));
        scope.setExtra("stream_duration_seconds", durationString);
        scope.setExtra("target_url", targetUrl);
        Optional.ofNullable(lastSuccessfulMessage).map(Object::toString)
          .ifPresent(message -> scope.setExtra("last_successful_message", message));
      });
    }
  }
}
