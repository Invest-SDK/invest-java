package ru.ttech.piapi.strategy.candle.backtest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CandleStrategyBacktest {

  private static final Logger logger = LoggerFactory.getLogger(CandleStrategyBacktest.class);
  private final CandleStrategyBacktestConfiguration config;
  private final BarsLoader barsLoader;

  public CandleStrategyBacktest(
    CandleStrategyBacktestConfiguration config,
    BarsLoader barsLoader
  ) {
    this.config = config;
    this.barsLoader = barsLoader;
  }

  public void run() {
    logger.info("Backtest started...");
    var barSeriesManager = config.createBarSeriesManager(barsLoader);
    config.getStrategyAnalysis().accept(barSeriesManager);
    logger.info("Backtest finished!");
  }
}
