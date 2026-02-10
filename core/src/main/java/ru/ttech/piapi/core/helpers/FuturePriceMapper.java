package ru.ttech.piapi.core.helpers;

import ru.tinkoff.piapi.contract.v1.Future;
import ru.tinkoff.piapi.contract.v1.FutureResponse;
import ru.tinkoff.piapi.contract.v1.Quotation;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;

/**
 * Маппер для конвертации цены фьючерсного контракта.
 *
 * <p>Позволяет выполнять двустороннюю конвертацию:
 * <ul>
 *   <li>из валюты в пункты</li>
 *   <li>из пунктов в валюту</li>
 * </ul>
 *
 * <p>Для расчета используются параметры спецификации инструмента:
 * <ul>
 *   <li>{@code min_price_increment} — минимальный шаг цены</li>
 *   <li>{@code min_price_increment_amount} — стоимость шага цены в валюте</li>
 * </ul>
 *
 * <p>Формулы конвертации:
 * <pre>{@code
 * price_pt = price_currency * min_price_increment / min_price_increment_amount
 *
 * price_currency = price_pt * min_price_increment_amount / min_price_increment
 * }</pre>
 */
public class FuturePriceMapper {

  private static final int SCALE = 9;

  /**
   * Конвертирует цену из валюты в пункты.
   *
   * <p>Например:<pre>{@code
   * price = 12.34
   * min_price_increment = 1
   * min_price_increment_amount = 0.01
   *
   * result ---> 1234
   * }</pre>
   *
   * @param price                   цена в валюте
   * @param minPriceIncrement       минимальный шаг цены
   * @param minPriceIncrementAmount стоимость шага цены в валюте
   * @return значение цены в пунктах
   */
  public static BigDecimal currencyToPoints(BigDecimal price,
                                            Quotation minPriceIncrement,
                                            Quotation minPriceIncrementAmount) {

    BigDecimal priceInc = NumberMapper.quotationToBigDecimal(minPriceIncrement);
    BigDecimal priceIncAmount = NumberMapper.quotationToBigDecimal(minPriceIncrementAmount);

    validate(priceInc, priceIncAmount);

    return Optional.ofNullable(price)
      .orElse(BigDecimal.ZERO)
      .multiply(priceInc)
      .divide(priceIncAmount, SCALE, RoundingMode.HALF_UP);
  }

  /**
   * Конвертирует цену из пунктов в валюту.
   *
   * <p>Например:<pre>{@code
   * price = 1234
   * min_price_increment = 1
   * min_price_increment_amount = 0.01
   *
   * result ---> 12.34
   * }</pre>
   *
   * @param price                   цена в пунктах
   * @param minPriceIncrement       минимальный шаг цены
   * @param minPriceIncrementAmount стоимость шага цены в валюте
   * @return значение цены в валюте
   */
  public static BigDecimal pointsToCurrency(BigDecimal price,
                                            Quotation minPriceIncrement,
                                            Quotation minPriceIncrementAmount) {

    BigDecimal priceInc = NumberMapper.quotationToBigDecimal(minPriceIncrement);
    BigDecimal priceIncAmount = NumberMapper.quotationToBigDecimal(minPriceIncrementAmount);

    validate(priceInc, priceIncAmount);

    return Optional.ofNullable(price)
      .orElse(BigDecimal.ZERO)
      .multiply(priceIncAmount)
      .divide(priceInc, SCALE, RoundingMode.HALF_UP);
  }

  /**
   * Конвертирует цену из валюты в пункты.
   *
   * <p>Например:<pre>{@code
   * price = 12.34
   * futureResponse = {
   *   instrument: {
   *     min_price_increment: 1
   *     min_price_increment_amount: 0.01
   *   }}
   *
   * result ---> 1234
   * }</pre>
   *
   * @param price          цена в валюте
   * @param futureResponse данные по фьючерсу
   * @return значение цены в пунктах
   */
  public static BigDecimal currencyToPoints(BigDecimal price,
                                            FutureResponse futureResponse) {

    Future future = Optional.ofNullable(futureResponse)
      .map(FutureResponse::getInstrument)
      .orElse(Future.getDefaultInstance());

    return currencyToPoints(
      price,
      future.getMinPriceIncrement(),
      future.getMinPriceIncrementAmount()
    );
  }

  /**
   * Конвертирует цену из пунктов в валюту.
   *
   * <p>Например:<pre>{@code
   * price = 1234
   * futureResponse = {
   *   instrument: {
   *     min_price_increment: 1
   *     min_price_increment_amount: 0.01
   *   }}
   *
   * result ---> 12.34
   * }</pre>
   *
   * @param price          цена в пунктах
   * @param futureResponse данные по фьючерсу
   * @return значение цены в валюте
   */
  public static BigDecimal pointsToCurrency(BigDecimal price,
                                            FutureResponse futureResponse) {

    Future future = Optional.ofNullable(futureResponse)
      .map(FutureResponse::getInstrument)
      .orElse(Future.getDefaultInstance());

    return pointsToCurrency(
      price,
      future.getMinPriceIncrement(),
      future.getMinPriceIncrementAmount()
    );
  }

  private static void validate(BigDecimal inc, BigDecimal incAmount) {
    if (inc.signum() <= 0 || incAmount.signum() <= 0) {
      throw new IllegalArgumentException("minPriceIncrement and minPriceIncrementAmount must be > 0");
    }
  }
}
