package ru.ttech.piapi.strategy.candle.live;

import org.junit.jupiter.api.Test;
import ru.tinkoff.piapi.contract.v1.Candle;
import ru.tinkoff.piapi.contract.v1.CandleInstrument;
import ru.tinkoff.piapi.contract.v1.SubscriptionInterval;
import ru.ttech.piapi.core.impl.marketdata.wrapper.CandleWrapper;

import static org.assertj.core.api.Assertions.assertThat;

class CandleStrategyTest {

  private static final String INSTRUMENT_UID = "test-instrument-uid";
  private static final String FIGI = "test-figi";
  private static final String TICKER = "TEST";
  private static final String CLASS_CODE = "TQBR";

  private final CandleWrapper candle = new CandleWrapper(
    Candle.newBuilder()
      .setInstrumentUid(INSTRUMENT_UID)
      .setFigi(FIGI)
      .setTicker(TICKER)
      .setClassCode(CLASS_CODE)
      .setInterval(SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
      .build()
  );

  @Test
  void matchesCandle_byInstrumentUid() {
    var instrument = candleInstrument(INSTRUMENT_UID);
    assertThat(CandleStrategy.matchesCandle(instrument, candle)).isTrue();
  }

  @Test
  void matchesCandle_byFigi() {
    var instrument = candleInstrument(FIGI);
    assertThat(CandleStrategy.matchesCandle(instrument, candle)).isTrue();
  }

  @Test
  void matchesCandle_byTickerAndClassCode() {
    var instrument = candleInstrument(TICKER + "_" + CLASS_CODE);
    assertThat(CandleStrategy.matchesCandle(instrument, candle)).isTrue();
  }

  @Test
  void matchesCandle_noMatch() {
    var instrument = candleInstrument("some-other-id");
    assertThat(CandleStrategy.matchesCandle(instrument, candle)).isFalse();
  }

  private static CandleInstrument candleInstrument(String instrumentId) {
    return CandleInstrument.newBuilder()
      .setInstrumentId(instrumentId)
      .setInterval(SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
      .build();
  }
}
