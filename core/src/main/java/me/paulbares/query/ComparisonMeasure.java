package me.paulbares.query;

import java.util.Map;
import java.util.Objects;

public class ComparisonMeasure implements Measure {

  public String alias;

  public String method;

  public Measure measure;

  public Map<String, String> referencePosition; // TODO support first and last

  /**
   * For jackson.
   */
  public ComparisonMeasure() {
  }

  public ComparisonMeasure(String alias,
                           String method,
                           Measure measure,
                           Map<String, String> referencePosition) {
    this.alias = alias;
    this.method = method;
    this.measure = measure;
    this.referencePosition = referencePosition;
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
    return Objects.equals(this.alias, that.alias)
            && this.method.equals(that.method)
            && this.measure.equals(that.measure)
            && this.referencePosition.equals(that.referencePosition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.alias, this.method, this.measure, this.referencePosition);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" +
            "alias='" + this.alias + '\'' +
            ", method='" + this.method + '\'' +
            ", measure=" + this.measure +
            ", referencePosition=" + this.referencePosition +
            '}';
  }
}
