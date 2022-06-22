package me.paulbares.query;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class BinaryOperationMeasure implements Measure {

  public static String KEY = "binary_operation";

  public String type;

  public String alias;

  public String method;

  public Measure measure; // FIXME support any

  public Map<String, String> referencePosition; // TODO support first and last

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
                                Measure measure,
                                Map<String, String> referencePosition) {
    this.alias = alias;
    this.method = method;
    this.measure = measure;
    this.referencePosition = referencePosition;
    this.type = KEY;
  }

//  public BinaryOperationMeasure(String alias,
//                                String method,
//                                Measure measure,
//                                Map<String, String> referencePosition) {
//    this(alias, method, measure, transform(referencePosition));
//  }

  private static Map<PeriodUnit, String> transform(Map<String, String> referencePosition) {
    Map<PeriodUnit, String> m = new HashMap<>();
    referencePosition.entrySet().forEach(e -> {
      m.put(PeriodUnit.valueOf(e.getKey()), e.getValue());
    });
    return m;
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
    return this.type.equals(that.type)
            && Objects.equals(this.alias, that.alias)
            && this.method.equals(that.method)
            && this.measure.equals(that.measure)
            && this.referencePosition.equals(that.referencePosition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.type, this.alias, this.method, this.measure, this.referencePosition);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" +
            "type='" + this.type + '\'' +
            ", alias='" + this.alias + '\'' +
            ", method='" + this.method + '\'' +
            ", measure=" + this.measure +
            ", referencePosition=" + this.referencePosition +
            '}';
  }
}
