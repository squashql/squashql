package io.squashql.query.measure;

import io.squashql.query.agg.AggregationFunction;

public class CountMeasure extends AggregatedMeasure {

  public static final CountMeasure INSTANCE = new CountMeasure();
  public static final String ALIAS = "_contributors_count_";
  public static final String FIELD_NAME = "*";

  /**
   * Default const.
   */
  private CountMeasure() {
    super(ALIAS, "*", AggregationFunction.COUNT);
  }
}
