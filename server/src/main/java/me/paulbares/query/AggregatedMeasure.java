package me.paulbares.query;

import java.util.Objects;

import static me.paulbares.query.sql.SQLTranslator.escape;

public class AggregatedMeasure implements Measure {

  public String field;

  public String aggregationFunction;

  /**
   * For jackson.
   */
  public AggregatedMeasure() {
  }

  public AggregatedMeasure(String field, String aggregationFunction) {
    this.field = Objects.requireNonNull(field);
    this.aggregationFunction = Objects.requireNonNull(aggregationFunction);
  }

  @Override
  public String sqlExpression() {
    return this.aggregationFunction + "(" + escape(this.field) + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AggregatedMeasure that = (AggregatedMeasure) o;
    return field.equals(that.field) && aggregationFunction.equals(that.aggregationFunction);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return "AggregatedMeasure{" +
            "name='" + field + '\'' +
            ", aggregationFunction='" + aggregationFunction + '\'' +
            '}';
  }
}
