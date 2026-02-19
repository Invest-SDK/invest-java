package ru.ttech.piapi.core.impl.marketdata.subscription;

import ru.tinkoff.piapi.contract.v1.Candle;
import ru.tinkoff.piapi.contract.v1.LastPrice;
import ru.tinkoff.piapi.contract.v1.OrderBook;
import ru.tinkoff.piapi.contract.v1.Trade;
import ru.tinkoff.piapi.contract.v1.TradingStatus;

import java.util.Map;

public class InstrumentMapper {
  public static Instrument map(Candle candle) {
    return new Instrument(Map.of(
      Instrument.IdType.INSTRUMENT_UID, candle.getInstrumentUid(),
      Instrument.IdType.TICKER_CLASSCODE, candle.getTicker() + "_" + candle.getClassCode(),
      Instrument.IdType.FIGI, candle.getFigi()
    ),
      candle.getInterval()
    );
  }

  public static Instrument map(LastPrice lastPrice) {
    return new Instrument(Map.of(
      Instrument.IdType.INSTRUMENT_UID, lastPrice.getInstrumentUid(),
      Instrument.IdType.TICKER_CLASSCODE, lastPrice.getTicker() + "_" + lastPrice.getClassCode(),
      Instrument.IdType.FIGI, lastPrice.getFigi()
    ));
  }

  public static Instrument map(Trade trade) {
    return new Instrument(Map.of(
      Instrument.IdType.INSTRUMENT_UID, trade.getInstrumentUid(),
      Instrument.IdType.TICKER_CLASSCODE, trade.getTicker() + "_" + trade.getClassCode(),
      Instrument.IdType.FIGI, trade.getFigi()
    ));
  }

  public static Instrument map(OrderBook orderBook) {
    return new Instrument(
      Map.of(
        Instrument.IdType.INSTRUMENT_UID, orderBook.getInstrumentUid(),
        Instrument.IdType.TICKER_CLASSCODE, orderBook.getTicker() + "_" + orderBook.getClassCode(),
        Instrument.IdType.FIGI, orderBook.getFigi()
      ),
      orderBook.getDepth(),
      orderBook.getOrderBookType()
    );
  }

  public static Instrument map(TradingStatus tradingStatus) {
    return new Instrument(Map.of(
      Instrument.IdType.INSTRUMENT_UID, tradingStatus.getInstrumentUid(),
      Instrument.IdType.TICKER_CLASSCODE, tradingStatus.getTicker() + "_" + tradingStatus.getClassCode(),
      Instrument.IdType.FIGI, tradingStatus.getFigi()
    ));
  }

}
