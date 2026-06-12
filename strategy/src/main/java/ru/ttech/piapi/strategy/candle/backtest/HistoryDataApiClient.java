package ru.ttech.piapi.strategy.candle.backtest;

import com.google.common.net.HttpHeaders;
import io.github.resilience4j.decorators.Decorators;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.vavr.control.Try;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.ttech.piapi.core.connector.ConnectorConfiguration;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;

public class HistoryDataApiClient {
  private static final Logger logger = LoggerFactory.getLogger(HistoryDataApiClient.class);
  private static final String HISTORY_DATA_URL = "https://invest-public-api.tbank.ru/history-data?instrumentId=%s&year=%d";
  private static final String FILENAME_PATTERN = "%s_%d.zip";
  private static final DateTimeFormatter ZIP_ENTRY_DTE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
  private final OkHttpClient client;
  private final String candlesDownloadPath;
  private final ConnectorConfiguration configuration;
  private final RateLimiterRegistry rateLimiterRegistry;

  public HistoryDataApiClient(String candlesDownloadPath, ConnectorConfiguration configuration) {
    this.candlesDownloadPath = candlesDownloadPath;
    this.configuration = configuration;
    this.client = createHttpClient(configuration);
    this.rateLimiterRegistry = RateLimiterRegistry.custom()
      .withRateLimiterConfig(RateLimiterConfig.custom()
        .limitRefreshPeriod(Duration.ofMinutes(1))
        .timeoutDuration(Duration.ofMinutes(1))
        .limitForPeriod(30)
        .build())
      .build();
  }

  public void downloadHistoricalDataArchive(String instrumentUid, int year) {
    String fileName = String.format(FILENAME_PATTERN, instrumentUid, year);
    if (candlesDownloadPath != null && !candlesDownloadPath.isBlank()) {
      fileName = candlesDownloadPath + fileName;
    }
    if (year < LocalDate.now().getYear() && checkFileExists(fileName)
      || year == LocalDate.now().getYear() && checkDataIsUpToDate(fileName)
    ) {
      return;
    }
    Request request = new Request.Builder()
      .url(String.format(HISTORY_DATA_URL, instrumentUid, year))
      .addHeader(HttpHeaders.AUTHORIZATION, String.format("Bearer %s", configuration.getToken()))
      .build();
    try {
      Response response = Decorators.ofCallable(() -> client.newCall(request).execute())
        .withRateLimiter(rateLimiterRegistry.rateLimiter("history-data"))
        .call();
      if (!response.isSuccessful()) {
        throw new IOException("Unexpected code " + response);
      }
      saveResponseToFile(response, fileName);
    } catch (IOException e) {
      logger.error("Error occurred while downloading data for instrument {} and year {}: {}", instrumentUid, year, e.getMessage());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void saveResponseToFile(Response response, String filename) throws IOException {
    if (response.body() == null) {
      throw new IOException("Response body is null " + response);
    }
    File zipFile = new File(filename);
    try (ResponseBody responseBody = response.body();
         InputStream inputStream = responseBody.byteStream();
         FileOutputStream fileOutputStream = new FileOutputStream(zipFile)) {
      byte[] buffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        fileOutputStream.write(buffer, 0, bytesRead);
      }
    }
    logger.debug("Zip-archive {}.zip saved successfully!", filename);
  }

  private boolean checkDataIsUpToDate(String fileName) {
    try (ZipFile zip = ZipFile.builder().setFile(fileName).get()) {
      var sortedEntries = Collections.list(zip.getEntries()).stream()
        .sorted(Comparator.comparing(ZipEntry::getName))
        .toList();
      var lastEntry = sortedEntries.getLast();
      var instrumentUidAndDate = lastEntry.getName().split("_");
      if (instrumentUidAndDate.length == 2) {
        var date = instrumentUidAndDate[1].substring(0, 8);
        if (LocalDate.parse(date, ZIP_ENTRY_DTE_FORMAT).plusDays(1).equals(LocalDate.now())) {
          logger.debug("File {} has up to date data, skipping download", fileName);
          return true;
        }
      }
      return false;
    } catch (IOException e) {
      logger.info("Error occurred while reading file {}!", fileName);
      return false;
    }
  }

  private boolean checkFileExists(String fileName) {
    File file = new File(fileName);
    if (file.exists() && !file.isDirectory()) {
      logger.debug("File {} already exists, skipping download", fileName);
      return true;
    }
    return false;
  }

  private static OkHttpClient createHttpClient(ConnectorConfiguration configuration) {
    var builder = new OkHttpClient.Builder();
    if (configuration.isSslInsecure()) {
      Try.of(() -> {
          var trustAllCerts = new X509TrustManager[]{
            new X509TrustManager() {
              public void checkClientTrusted(X509Certificate[] chain, String authType) {}
              public void checkServerTrusted(X509Certificate[] chain, String authType) {}
              public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            }
          };
          var sslContext = SSLContext.getInstance("TLS");
          sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
          return builder
            .sslSocketFactory(sslContext.getSocketFactory(), trustAllCerts[0])
            .hostnameVerifier((hostname, session) -> true);
        })
        .onFailure(ex -> logger.warn("Failed to configure insecure SSL for HTTP client, using default", ex));
    }
    return builder.build();
  }
}
