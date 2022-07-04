package me.paulbares.query;

import java.util.Objects;

import static me.paulbares.query.SqlUtils.escape;

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
    return this.aggregationFunction + "(" + (this.field.equals("*") ? this.field : escape(this.field)) + ")";
  }

  @Override
  public String alias() {
    return this.aggregationFunction + "(" + this.field + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AggregatedMeasure that = (AggregatedMeasure) o;
    return Objects.equals(this.field, that.field) && Objects.equals(this.aggregationFunction, that.aggregationFunction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.field, this.aggregationFunction);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() +
            "{" +
            "field='" + field + '\'' +
            ", aggregationFunction='" + aggregationFunction + '\'' +
            '}';
  }
}
