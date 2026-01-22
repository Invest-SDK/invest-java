package ru.ttech.piapi.strategy.candle.backtest;

import org.jspecify.annotations.NonNull;
import org.ta4j.core.Bar;
import org.ta4j.core.BarSeries;
import org.ta4j.core.BaseBarSeriesBuilder;
import org.ta4j.core.analysis.cost.CostModel;
import org.ta4j.core.analysis.cost.LinearTransactionCostModel;
import org.ta4j.core.analysis.cost.ZeroCostModel;
import org.ta4j.core.backtest.BarSeriesManager;
import org.ta4j.core.backtest.TradeExecutionModel;
import org.ta4j.core.backtest.TradeOnCurrentCloseModel;
import org.ta4j.core.num.DecimalNumFactory;
import ru.tinkoff.piapi.contract.v1.CandleInterval;
import ru.ttech.piapi.strategy.candle.mapper.BarMapper;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CandleStrategyBacktestConfiguration {

  private final ExecutorService executorService;
  private final Function<BarsLoader, BarSeriesManager> barSeriesManagerCreator;
  private final Consumer<BarSeriesManager> strategyAnalysis;

  private CandleStrategyBacktestConfiguration(
    ExecutorService executorService,
    Function<BarsLoader, BarSeriesManager> barSeriesManagerCreator,
    Consumer<BarSeriesManager> strategyAnalysis
  ) {
    this.executorService = executorService;
    this.strategyAnalysis = strategyAnalysis;
    this.barSeriesManagerCreator = barSeriesManagerCreator;
  }

  protected BarSeriesManager createBarSeriesManager(BarsLoader barsLoader) {
    return barSeriesManagerCreator.apply(barsLoader);
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Consumer<BarSeriesManager> getStrategyAnalysis() {
    return strategyAnalysis;
  }

  public static class Builder {

    private String instrumentId;
    private CandleInterval candleInterval;
    private CostModel tradeFeeModel = new LinearTransactionCostModel(0.003);
    private TradeExecutionModel tradeExecutionModel = new TradeOnCurrentCloseModel();
    private ExecutorService executorService;
    private LocalDate from;
    private LocalDate to;
    private BarSeriesManager barSeriesManager;
    private Consumer<BarSeriesManager> strategyAnalysis;

    public Builder setInstrumentId(@NonNull String instrumentId) {
      this.instrumentId = instrumentId;
      return this;
    }

    public Builder setCandleInterval(@NonNull CandleInterval candleInterval) {
      this.candleInterval = candleInterval;
      return this;
    }

    public Builder setTradeFeeModel(CostModel costModel) {
      this.tradeFeeModel = costModel;
      return this;
    }

    public Builder setTradeExecutionModel(TradeExecutionModel tradeExecutionModel) {
      this.tradeExecutionModel = tradeExecutionModel;
      return this;
    }

    public Builder setExecutorService(@NonNull ExecutorService executorService) {
      this.executorService = executorService;
      return this;
    }

    public Builder setFrom(@NonNull LocalDate from) {
      this.from = from;
      return this;
    }

    public Builder setTo(@NonNull LocalDate to) {
      this.to = to;
      return this;
    }

    /**
     * Устанавливает источник свечей.
     * Если задан, параметры from, to, instrumentId и candleInterval игнорируются и исторические свечи не загружаются.
     * @param barSeriesManager источник свечей
     */
    public Builder setBarSeriesManager(BarSeriesManager barSeriesManager) {
      this.barSeriesManager = barSeriesManager;
      return this;
    }

    public Builder setStrategyAnalysis(@NonNull Consumer<BarSeriesManager> strategyAnalysis) {
      this.strategyAnalysis = strategyAnalysis;
      return this;
    }

    public CandleStrategyBacktestConfiguration build() {
      final Function<BarsLoader, BarSeriesManager> barSeriesManagerCreator;
      if (executorService == null) {
        throw new IllegalArgumentException("'executorService' should be set!");
      }
      if (barSeriesManager == null) {
        if (instrumentId == null || candleInterval == null || from == null || to == null) {
          throw new IllegalArgumentException("'instrumentId', 'candleInterval', 'from' and 'to' should be set!");
        }
        if (from.isAfter(to)) {
          throw new IllegalArgumentException("'from' should be before 'to'!");
        }
        barSeriesManagerCreator = barsLoader -> {
          var barsData = barsLoader.loadBars(instrumentId, candleInterval, from, to);
          List<Bar> bars = StreamSupport.stream(barsData.spliterator(), false)
            .map(barData -> BarMapper.mapBarDataWithIntervalToBar(barData, candleInterval))
            .collect(Collectors.toList());
          BarSeries barSeries = new BaseBarSeriesBuilder()
            .withNumFactory(DecimalNumFactory.getInstance())
            .withBars(bars)
            .build();
          return new BarSeriesManager(barSeries, tradeFeeModel, new ZeroCostModel(), tradeExecutionModel);
        };
      } else {
        barSeriesManagerCreator = barsLoader -> barSeriesManager;
      }
      return new CandleStrategyBacktestConfiguration(executorService, barSeriesManagerCreator, strategyAnalysis);
    }
  }
}
