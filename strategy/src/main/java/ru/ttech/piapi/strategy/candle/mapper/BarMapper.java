package ru.ttech.piapi.strategy.candle.mapper;

import org.ta4j.core.Bar;
import org.ta4j.core.BaseBar;
import org.ta4j.core.num.DecimalNum;
import org.ta4j.core.num.DoubleNum;
import ru.tinkoff.piapi.contract.v1.CandleInterval;
import ru.tinkoff.piapi.contract.v1.HistoricCandle;
import ru.tinkoff.piapi.contract.v1.SubscriptionInterval;
import ru.ttech.piapi.core.helpers.NumberMapper;
import ru.ttech.piapi.core.helpers.TimeMapper;
import ru.ttech.piapi.core.impl.marketdata.wrapper.CandleWrapper;
import ru.ttech.piapi.strategy.candle.backtest.BarData;
import ru.ttech.piapi.strategy.candle.backtest.TimeHelper;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class BarMapper {

  public static Bar mapCandleWrapperToBar(CandleWrapper candle) {
    var startTime = candle.getTime().toInstant(ZoneOffset.UTC);
    var period = PeriodMapper.getTimePeriod(startTime, candle.getInterval());
    var endTime = startTime.plus(period);
    return new BaseBar(period, startTime, endTime,
      DecimalNum.valueOf(candle.getOpen()),
      DecimalNum.valueOf(candle.getHigh()),
      DecimalNum.valueOf(candle.getLow()),
      DecimalNum.valueOf(candle.getClose()),
      DecimalNum.valueOf(candle.getVolume()),
      DoubleNum.ZERO,
      0
    );
  }

  public static Bar mapHistoricCandleToBar(HistoricCandle candle, SubscriptionInterval interval) {
    var startTime = TimeMapper.timestampToInstant(candle.getTime());
    var period = PeriodMapper.getTimePeriod(startTime, interval);
    var endTime = startTime.plus(period);
    return new BaseBar(period, startTime, endTime,
      DecimalNum.valueOf(DecimalNum.valueOf(NumberMapper.quotationToBigDecimal(candle.getOpen()))),
      DecimalNum.valueOf(DecimalNum.valueOf(NumberMapper.quotationToBigDecimal(candle.getHigh()))),
      DecimalNum.valueOf(DecimalNum.valueOf(NumberMapper.quotationToBigDecimal(candle.getLow()))),
      DecimalNum.valueOf(DecimalNum.valueOf(NumberMapper.quotationToBigDecimal(candle.getClose()))),
      DecimalNum.valueOf(candle.getVolume()),
      DoubleNum.ZERO,
      0
    );
  }

  public static Bar mapBarDataWithIntervalToBar(BarData barData, CandleInterval interval) {
    var startTime = TimeHelper.roundFloorStartTime(ZonedDateTime.parse(barData.getStartTime()), interval);
    var endTime = TimeHelper.getEndTime(startTime, interval);
    var period = Duration.between(startTime, endTime);
    return new BaseBar(period, startTime.toInstant(), endTime.toInstant(),
      DecimalNum.valueOf(DecimalNum.valueOf(barData.getOpen())),
      DecimalNum.valueOf(DecimalNum.valueOf(barData.getHigh())),
      DecimalNum.valueOf(DecimalNum.valueOf(barData.getLow())),
      DecimalNum.valueOf(DecimalNum.valueOf(barData.getClose())),
      DecimalNum.valueOf(DecimalNum.valueOf(barData.getVolume())),
      DoubleNum.ZERO,
      0
    );
  }
}
