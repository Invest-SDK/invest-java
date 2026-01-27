package ru.ttech.piapi.core.impl.marketdata.subscription;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.jspecify.annotations.NonNull;
import ru.tinkoff.piapi.contract.v1.OrderBookType;
import ru.tinkoff.piapi.contract.v1.SubscriptionInterval;

import java.util.EnumMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Инструмент для подписки на рыночные данные
 * <p><b>Для подписки на разные типы данных необходимо использовать разные конструкторы</b>
 */
@Getter
@ToString
@EqualsAndHashCode
public class Instrument {

  private final InstrumentId instrumentId;
  private final SubscriptionInterval subscriptionInterval;
  private final int depth;
  private final OrderBookType orderBookType;

  /**
   * Конструктор инструмента для подписки на последнюю цену, сделки и торговый статус
   *
   * @param instrumentUid UID инструмента
   */
  public Instrument(String instrumentUid) {
    this.instrumentId = new InstrumentId(instrumentUid);
    this.depth = 0;
    this.subscriptionInterval = null;
    this.orderBookType = null;
  }

  /**
   * Конструктор инструмента для подписки на последнюю цену, сделки и торговый статус
   *
   * @param ticker тикер инструмента
   * @param classCode класс-код инструмента
   */
  public Instrument(String ticker, String classCode) {
    this.instrumentId = new InstrumentId(ticker, classCode);
    this.depth = 0;
    this.subscriptionInterval = null;
    this.orderBookType = null;
  }

  Instrument(Map<IdType, String> ids) {
    this.instrumentId = new InstrumentId(new EnumMap<>(ids));
    this.depth = 0;
    this.subscriptionInterval = null;
    this.orderBookType = null;
  }

  /**
   * Конструктор инструмента для подписки на свечи
   *
   * @param instrumentUid UID инструмента
   * @param subscriptionInterval Период одной свечи
   */
  public Instrument(String instrumentUid, SubscriptionInterval subscriptionInterval) {
    this.instrumentId = new InstrumentId(instrumentUid);
    this.subscriptionInterval = subscriptionInterval;
    this.orderBookType = null;
    this.depth = 0;
  }

  /**
   * Конструктор инструмента для подписки на свечи
   *
   * @param ticker тикер инструмента
   * @param classCode класс-код инструмента
   * @param subscriptionInterval Период одной свечи
   */
  public Instrument(String ticker, String classCode, SubscriptionInterval subscriptionInterval) {
    this.instrumentId = new InstrumentId(ticker, classCode);
    this.subscriptionInterval = subscriptionInterval;
    this.orderBookType = null;
    this.depth = 0;
  }

  Instrument(Map<IdType, String> ids, SubscriptionInterval subscriptionInterval) {
    this.instrumentId = new InstrumentId(new EnumMap<>(ids));
    this.subscriptionInterval = subscriptionInterval;
    this.orderBookType = null;
    this.depth = 0;
  }

  /**
   * Конструктор инструмента для подписки на торговый стакан
   *
   * @param instrumentUid UID инструмента
   * @param depth Глубина стакана
   * @param orderBookType Тип стакана
   */
  public Instrument(String instrumentUid, int depth, OrderBookType orderBookType) {
    this.instrumentId = new InstrumentId(instrumentUid);
    this.depth = depth;
    this.orderBookType = orderBookType;
    this.subscriptionInterval = null;
  }

  /**
   * Конструктор инструмента для подписки на торговый стакан
   *
   * @param ticker тикер инструмента
   * @param classCode класс-код инструмента
   * @param depth Глубина стакана
   * @param orderBookType Тип стакана
   */
  public Instrument(String ticker, String classCode, int depth, OrderBookType orderBookType) {
    this.instrumentId = new InstrumentId(ticker, classCode);
    this.depth = depth;
    this.orderBookType = orderBookType;
    this.subscriptionInterval = null;
  }

  Instrument(Map<IdType, String> ids, int depth, OrderBookType orderBookType) {
    this.instrumentId = new InstrumentId(new EnumMap<>(ids));
    this.depth = depth;
    this.orderBookType = orderBookType;
    this.subscriptionInterval = null;
  }

  /**
   * Метод проверяет, что подписка на обновления по инструменту идентична другой подписке.
   * Для того, чтобы подписки были идентичными, необходимо, чтобы у них совпадали:
   * <li>Хотя бы один идентификатор</li>
   * <li>Все параметры подписки: subscriptionInterval, depth, orderBookType</li>
   *
   * @param instrument Подписка для сравнения
   * @return true, если у инструментов совпадает хотя бы один id
   */
  public boolean subscriptionEquals(Instrument instrument) {
    if (instrument == null) {
      return false;
    }
    return instrumentId.getIds().containsValue(instrument.getInstrumentId().id())
      && instrument.getSubscriptionInterval() == subscriptionInterval
      && instrument.getOrderBookType() == orderBookType
      && instrument.getDepth() == depth;
  }

  @Getter
  @EqualsAndHashCode
  public static class InstrumentId {

    private final Map<IdType, String> ids;

    InstrumentId(String instrumentId) {
      this.ids = new EnumMap<>(Map.of(
        IdType.UNKNOWN, instrumentId
      ));
    }

    InstrumentId(String ticker, String classCode) {
      this.ids = new EnumMap<>(Map.of(
        IdType.UNKNOWN, ticker + "_" + classCode
      ));
    }

    InstrumentId(Map<IdType, String> ids) {
      this.ids = ids;
    }

    /**
     * Метод возвращает первый в списке id инструмента. Порядок соответствует перечислению {@link IdType}
     *
     * @return id инструмента
     */
    public String id() {
      if (ids == null || ids.isEmpty()) {
        return null;
      }
      return ids.values().iterator().next();
    }

    @NonNull
    @Override
    public String toString() {
      if (ids == null || ids.isEmpty()) {
        return "InstrumentId{}";
      }
      String content = ids.entrySet().stream()
        .map(e -> e.getKey() + "=" + e.getValue())
        .collect(Collectors.joining(", "));

      return "InstrumentId{" + content + "}";
    }
  }

  public enum IdType {
    INSTRUMENT_UID,
    TICKER_CLASSCODE,
    FIGI,
    UNKNOWN
  }
}
