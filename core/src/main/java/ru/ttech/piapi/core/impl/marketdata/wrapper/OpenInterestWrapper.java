package ru.ttech.piapi.core.impl.marketdata.wrapper;

import ru.tinkoff.piapi.contract.v1.OpenInterest;
import ru.ttech.piapi.core.helpers.TimeMapper;
import ru.ttech.piapi.core.impl.wrapper.ResponseWrapper;

import java.time.LocalDateTime;

/**
 * Информация об открытом интересе для фьючерсов
 * <p>Обертка для работы с {@link OpenInterest}
 */
public class OpenInterestWrapper extends ResponseWrapper<OpenInterest> {

  public OpenInterestWrapper(OpenInterest response) {
    super(response);
  }

  /**
   * Метод для получения тикера инструмента
   *
   * @return тикер инструмента
   */
  public String getTicker() {
    return response.getTicker();
  }

  /**
   * Метод для получения класс-кода инструмента
   *
   * @return класс-код инструмента
   */
  public String getClassCode() {
    return response.getClassCode();
  }

  /**
   * Метод для получения открытого интереса по фьючерсу
   *
   * @return открытый интерес
   */
  public long getOpenInterest() {
    return response.getOpenInterest();
  }

  /**
   * Метод для получения UID инструмента
   *
   * @return UID инструмента
   */
  public String getInstrumentUid() {
    return response.getInstrumentUid();
  }

  /**
   * Метод для получения времени открытого интереса в часовом поясе UTC по времени биржи
   *
   * @return Время открытого интереса
   */
  public LocalDateTime getTime() {
    return TimeMapper.timestampToLocalDateTime(response.getTime());
  }
}
