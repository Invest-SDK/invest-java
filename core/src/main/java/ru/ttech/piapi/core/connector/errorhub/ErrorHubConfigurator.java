package ru.ttech.piapi.core.connector.errorhub;

import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.sentry.Sentry;
import io.sentry.SentryOptions;
import lombok.extern.slf4j.Slf4j;
import ru.ttech.piapi.core.connector.internal.ErrorHubReportInterceptor;

import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

@Slf4j
public class ErrorHubConfigurator {

  private static final String TTECH_DSN;
  private static final double SAMPLE_RATE = 1.0;
  private static final int MAX_BREADCRUMBS = 3;
  private static final boolean ATTACH_STACKTRACE = true;
  private static final int MAX_DEPTH = 3;
  private static final String RELEASE_VERSION;
  private static final boolean SEND_MODULES = false;
  private static final Set<Class<? extends Throwable>> EXCEPTION_WHITELIST = Set.of(
    StatusRuntimeException.class,
    StatusException.class
  );

  static {
    Properties prop = new Properties();
    try (InputStream input = ErrorHubReportInterceptor.class.getClassLoader().getResourceAsStream("pom.properties")) {
      prop.load(input);
    } catch (Exception e) {
      log.error("Error while loading config.properties", e);
    }
    RELEASE_VERSION = Optional.ofNullable(prop.getProperty("errorhub.version")).orElseThrow();
    TTECH_DSN = Optional.ofNullable(prop.getProperty("errorhub.dsn")).orElseThrow();
    Sentry.init(ErrorHubConfigurator::configure);
  }

  public static void configure(SentryOptions options) {
    options.setDsn(TTECH_DSN);
    options.setRelease(RELEASE_VERSION);
    options.setSampleRate(SAMPLE_RATE);
    options.setMaxBreadcrumbs(MAX_BREADCRUMBS);
    options.setAttachStacktrace(ATTACH_STACKTRACE);
    options.setMaxDepth(MAX_DEPTH);
    options.setSendModules(SEND_MODULES);
    options.setBeforeSend((event, hint) -> {
      if (event.getThrowable() != null && EXCEPTION_WHITELIST.contains(event.getThrowable().getClass())) {
        return event;
      }
      return null;
    });
  }
}
