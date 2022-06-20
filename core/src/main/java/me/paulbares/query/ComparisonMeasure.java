package me.paulbares.query;

import java.util.Map;
import java.util.Objects;

public class ComparisonMeasure implements Measure {

  public static String KEY = "comparison";

  public String type;

  public String alias;

  public String method;

  public AggregatedMeasure measure;

  public Map<PeriodUnit, String> referencePosition; // TODO support first and last

  public enum PeriodUnit {
    MONTH,
    QUARTER,
    SEMESTER,
    YEAR
  }

  /**
   * For jackson.
   */
  public ComparisonMeasure() {
    this.type = KEY;
  }

  public ComparisonMeasure(String alias,
                           String method,
                           AggregatedMeasure measure,
                           Map<PeriodUnit, String> referencePosition) {
    this.alias = alias;
    this.method = method;
    this.measure = measure;
    this.referencePosition = referencePosition;
    this.type = KEY;
  }

  @Override
  public String sqlExpression() {
    throw new IllegalStateException();
  }

  @Override
  public String alias() {
    return this.alias; // TODO return default name if null
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ComparisonMeasure that = (ComparisonMeasure) o;
    return type.equals(that.type)
            && Objects.equals(alias, that.alias)
            && method.equals(that.method)
            && measure.equals(that.measure)
            && referencePosition.equals(that.referencePosition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, alias, method, measure, referencePosition);
  }
}
