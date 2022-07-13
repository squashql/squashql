package me.paulbares.query;

import me.paulbares.query.agg.AggregationFunction;

public class CountMeasure extends AggregatedMeasure {

  public static final CountMeasure INSTANCE = new CountMeasure();
  public static final String ALIAS = "_contributors_count_";
  public static final String field = "*";
  public static final String aggregationFunction = AggregationFunction.COUNT;

  /**
   * Default const.
   */
  private CountMeasure() {
    super(field, aggregationFunction);
    this.alias = ALIAS;
  }
}
