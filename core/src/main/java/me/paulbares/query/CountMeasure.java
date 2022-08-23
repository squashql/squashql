package me.paulbares.query;

import me.paulbares.query.agg.AggregationFunction;

public class CountMeasure extends AggregatedMeasure {

  public static final CountMeasure INSTANCE = new CountMeasure();
  public static final String ALIAS = "_contributors_count_";

  /**
   * Default const.
   */
  private CountMeasure() {
    super(ALIAS, "*", AggregationFunction.COUNT);
  }
}
