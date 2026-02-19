package ru.ttech.piapi.core.impl.marketdata.util;

import ru.tinkoff.piapi.contract.v1.MarketDataResponse;
import ru.ttech.piapi.core.impl.marketdata.MarketDataResponseType;
import ru.ttech.piapi.core.impl.marketdata.subscription.Instrument;
import ru.ttech.piapi.core.impl.marketdata.subscription.InstrumentMapper;
import ru.ttech.piapi.core.impl.marketdata.subscription.MarketDataSubscriptionResult;
import ru.ttech.piapi.core.impl.marketdata.subscription.SubscriptionResultMapper;

import java.util.Optional;

public class MarketDataResponseUtil {

  public static Optional<MarketDataSubscriptionResult> getSubscriptionResultFromResponse(MarketDataResponse response) {
    if (response.hasSubscribeCandlesResponse()) {
      return Optional.of(SubscriptionResultMapper.map(response.getSubscribeCandlesResponse()));
    } else if (response.hasSubscribeLastPriceResponse()) {
      return Optional.of(SubscriptionResultMapper.map(response.getSubscribeLastPriceResponse()));
    } else if (response.hasSubscribeOrderBookResponse()) {
      return Optional.of(SubscriptionResultMapper.map(response.getSubscribeOrderBookResponse()));
    } else if (response.hasSubscribeTradesResponse()) {
      return Optional.of(SubscriptionResultMapper.map(response.getSubscribeTradesResponse()));
    } else if (response.hasSubscribeInfoResponse()) {
      return Optional.of(SubscriptionResultMapper.map(response.getSubscribeInfoResponse()));
    }
    return Optional.empty();
  }

  public static Optional<MarketDataResponseType> getResponseType(MarketDataResponse response) {
    if (response.hasCandle()) {
      return Optional.of(MarketDataResponseType.CANDLE);
    } else if (response.hasLastPrice()) {
      return Optional.of(MarketDataResponseType.LAST_PRICE);
    } else if (response.hasOrderbook()) {
      return Optional.of(MarketDataResponseType.ORDER_BOOK);
    } else if (response.hasTrade()) {
      return Optional.of(MarketDataResponseType.TRADE);
    } else if (response.hasTradingStatus()) {
      return Optional.of(MarketDataResponseType.TRADING_STATUS);
    }
    return Optional.empty();
  }

  public static Optional<Instrument> extractInstrument(MarketDataResponse response) {

    if (response.hasCandle()) {
      var candle = response.getCandle();

      return Optional.of(InstrumentMapper.map(candle));
    }
    if (response.hasLastPrice()) {
      var lastPrice = response.getLastPrice();
      return Optional.of(InstrumentMapper.map(lastPrice));
    }
    if (response.hasTrade()) {
      var trade = response.getTrade();
      return Optional.of(InstrumentMapper.map(trade));
    }
    if (response.hasOrderbook()) {
      var orderbook = response.getOrderbook();
      return Optional.of(InstrumentMapper.map(orderbook));
    }
    if (response.hasTradingStatus()) {
      var tradingStatus = response.getTradingStatus();
      return Optional.of(InstrumentMapper.map(tradingStatus));
    }
    return Optional.empty();
  }
}
