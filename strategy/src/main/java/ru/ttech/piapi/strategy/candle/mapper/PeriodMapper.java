package ru.ttech.piapi.strategy.candle.mapper;

import ru.tinkoff.piapi.contract.v1.SubscriptionInterval;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class PeriodMapper {

  public static Duration getTimePeriod(Instant periodStart, SubscriptionInterval interval) {
    return switch (interval) {
      case SUBSCRIPTION_INTERVAL_ONE_MINUTE -> Duration.ofMinutes(1);
      case SUBSCRIPTION_INTERVAL_2_MIN -> Duration.ofMinutes(2);
      case SUBSCRIPTION_INTERVAL_3_MIN -> Duration.ofMinutes(3);
      case SUBSCRIPTION_INTERVAL_FIVE_MINUTES -> Duration.ofMinutes(5);
      case SUBSCRIPTION_INTERVAL_10_MIN -> Duration.ofMinutes(10);
      case SUBSCRIPTION_INTERVAL_FIFTEEN_MINUTES -> Duration.ofMinutes(15);
      case SUBSCRIPTION_INTERVAL_30_MIN -> Duration.ofMinutes(30);
      case SUBSCRIPTION_INTERVAL_ONE_HOUR -> Duration.ofHours(1);
      case SUBSCRIPTION_INTERVAL_2_HOUR -> Duration.ofHours(2);
      case SUBSCRIPTION_INTERVAL_4_HOUR -> Duration.ofHours(4);
      case SUBSCRIPTION_INTERVAL_ONE_DAY -> Duration.ofDays(1);
      case SUBSCRIPTION_INTERVAL_WEEK -> Duration.ofDays(7);
      case SUBSCRIPTION_INTERVAL_MONTH -> {
        var zonedEnd = periodStart.atZone(ZoneOffset.UTC).plusMonths(1);
        yield Duration.between(periodStart, zonedEnd.toInstant());
      }
      default -> throw new IllegalArgumentException("Unsupported interval");
    };
  }

  /**
   * <a href=https://russianinvestments.github.io/investAPI/load_history/>
   * О лимитах на загрузку свечей
   * </a>
   */
  public static LocalDateTime getStartTime(LocalDateTime endTime, SubscriptionInterval interval) {
    return switch (interval) {
      case SUBSCRIPTION_INTERVAL_ONE_MINUTE, SUBSCRIPTION_INTERVAL_2_MIN, SUBSCRIPTION_INTERVAL_3_MIN ->
        endTime.minusDays(1);
      case SUBSCRIPTION_INTERVAL_FIVE_MINUTES, SUBSCRIPTION_INTERVAL_10_MIN -> endTime.minusWeeks(1);
      case SUBSCRIPTION_INTERVAL_FIFTEEN_MINUTES, SUBSCRIPTION_INTERVAL_30_MIN -> endTime.minusWeeks(3);
      case SUBSCRIPTION_INTERVAL_ONE_HOUR, SUBSCRIPTION_INTERVAL_2_HOUR, SUBSCRIPTION_INTERVAL_4_HOUR ->
        endTime.minusMonths(3);
      case SUBSCRIPTION_INTERVAL_ONE_DAY -> endTime.minusYears(6);
      case SUBSCRIPTION_INTERVAL_WEEK -> endTime.minusYears(5);
      case SUBSCRIPTION_INTERVAL_MONTH -> endTime.minusYears(10);
      default -> throw new IllegalArgumentException("Unsupported interval");
    };
  }
}
