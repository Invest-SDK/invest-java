package ru.ttech.piapi.strategy.candle.backtest;

import ru.tinkoff.piapi.contract.v1.CandleInterval;

import java.time.DayOfWeek;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;

public class TimeHelper {

  public static ZonedDateTime roundFloorStartTime(ZonedDateTime startTime, CandleInterval candleInterval) {
    return switch (candleInterval) {
      case CANDLE_INTERVAL_2_MIN -> startTime.withMinute(startTime.getMinute() - startTime.getMinute() % 2);
      case CANDLE_INTERVAL_3_MIN -> startTime.withMinute(startTime.getMinute() - startTime.getMinute() % 3);
      case CANDLE_INTERVAL_5_MIN -> startTime.withMinute(startTime.getMinute() - startTime.getMinute() % 5);
      case CANDLE_INTERVAL_10_MIN -> startTime.withMinute(startTime.getMinute() - startTime.getMinute() % 10);
      case CANDLE_INTERVAL_15_MIN -> startTime.withMinute(startTime.getMinute() - startTime.getMinute() % 15);
      case CANDLE_INTERVAL_30_MIN -> startTime.withMinute(startTime.getMinute() - startTime.getMinute() % 30);
      case CANDLE_INTERVAL_HOUR -> startTime.truncatedTo(ChronoUnit.HOURS);
      case CANDLE_INTERVAL_2_HOUR ->
        startTime.truncatedTo(ChronoUnit.HOURS).withHour(startTime.getHour() - startTime.getHour() % 2);
      case CANDLE_INTERVAL_4_HOUR ->
        startTime.truncatedTo(ChronoUnit.HOURS).withHour(startTime.getHour() - startTime.getHour() % 4);
      case CANDLE_INTERVAL_DAY -> startTime.withHour(0).withMinute(0);
      case CANDLE_INTERVAL_WEEK ->
        startTime.withHour(0).withMinute(0).with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
      case CANDLE_INTERVAL_MONTH -> startTime.withHour(0).withMinute(0).withDayOfMonth(1);
      default -> startTime;
    };
  }

  public static ZonedDateTime getEndTime(ZonedDateTime startTime, CandleInterval candleInterval) {
    return switch (candleInterval) {
      case CANDLE_INTERVAL_1_MIN -> startTime.plusMinutes(1);
      case CANDLE_INTERVAL_2_MIN -> startTime.plusMinutes(2);
      case CANDLE_INTERVAL_3_MIN -> startTime.plusMinutes(3);
      case CANDLE_INTERVAL_5_MIN -> startTime.plusMinutes(5);
      case CANDLE_INTERVAL_10_MIN -> startTime.plusMinutes(10);
      case CANDLE_INTERVAL_15_MIN -> startTime.plusMinutes(15);
      case CANDLE_INTERVAL_30_MIN -> startTime.plusMinutes(30);
      case CANDLE_INTERVAL_HOUR -> startTime.truncatedTo(ChronoUnit.HOURS).plusHours(1);
      case CANDLE_INTERVAL_2_HOUR -> startTime.truncatedTo(ChronoUnit.HOURS).plusHours(2);
      case CANDLE_INTERVAL_4_HOUR -> startTime.truncatedTo(ChronoUnit.HOURS).plusHours(4);
      case CANDLE_INTERVAL_DAY -> startTime.truncatedTo(ChronoUnit.DAYS).plusDays(1);
      case CANDLE_INTERVAL_WEEK -> startTime.truncatedTo(ChronoUnit.DAYS).plusWeeks(1);
      case CANDLE_INTERVAL_MONTH -> startTime.truncatedTo(ChronoUnit.DAYS).plusMonths(1);
      default -> startTime;
    };
  }
}
