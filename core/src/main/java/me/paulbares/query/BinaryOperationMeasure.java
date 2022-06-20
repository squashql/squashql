package me.paulbares.query;

import java.util.Map;
import java.util.Objects;

public class BinaryOperationMeasure implements Measure {

  public static String KEY = "binary_operation";

  public String type;

  public String alias;

  public String method;

  public AggregatedMeasure measure; // FIXME support any

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
  public BinaryOperationMeasure() {
    this.type = KEY;
  }

  public BinaryOperationMeasure(String alias,
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
    BinaryOperationMeasure that = (BinaryOperationMeasure) o;
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
