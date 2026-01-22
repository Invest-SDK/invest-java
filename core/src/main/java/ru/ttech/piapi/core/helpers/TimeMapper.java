package ru.ttech.piapi.core.helpers;

import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.function.Function;

/**
 * Маппер для конвертации объектов времени и даты
 */
public class TimeMapper {

  private static final ZoneOffset DEFAULT_ZONE_OFFSET = ZoneOffset.UTC;
  private static final Function<Timestamp, LocalDateTime> convertToLocalDateTime =
    val -> mapSecondsAndNanos(val.getSeconds(), val.getNanos()).atZone(DEFAULT_ZONE_OFFSET).toLocalDateTime();
  private static final Function<Timestamp, Instant> convertToInstant =
    val -> mapSecondsAndNanos(val.getSeconds(), val.getNanos());

  /**
   * Преобразование {@link Timestamp} в {@link Instant}.
   *
   * @param timestamp Экземпляр google {@link Timestamp}.
   * @return Эквивалентный {@link Instant}.
   */
  public static Instant timestampToInstant(Timestamp timestamp) {
    return Optional.ofNullable(timestamp)
      .map(convertToInstant)
      .orElseGet(() -> convertToInstant.apply(Timestamp.getDefaultInstance()));
  }

  /**
   * Преобразование {@link Timestamp} в {@link LocalDateTime}.
   *
   * @param timestamp Экземпляр google {@link Timestamp}.
   * @return Эквивалентный {@link LocalDateTime} в зоне UTC.
   */
  public static LocalDateTime timestampToLocalDateTime(Timestamp timestamp) {
    return Optional.ofNullable(timestamp)
      .map(convertToLocalDateTime)
      .orElseGet(() -> convertToLocalDateTime.apply(Timestamp.getDefaultInstance()));
  }

  /**
   * Преобразование {@link LocalDateTime} в {@link Timestamp}.
   *
   * @param localDateTime Экземпляр {@link LocalDateTime}.
   * @return Эквивалентный google {@link Timestamp}.
   */
  public static Timestamp localDateTimeToTimestamp(LocalDateTime localDateTime) {
    return Optional.ofNullable(localDateTime).map(value ->
        Timestamp.newBuilder()
          .setSeconds(value.toEpochSecond(DEFAULT_ZONE_OFFSET))
          .setNanos(value.getNano())
          .build())
      .orElse(Timestamp.getDefaultInstance());
  }

  private static Instant mapSecondsAndNanos(long seconds, long nanos) {
    return Instant.ofEpochSecond(seconds, nanos);
  }
}
