package me.paulbares.query;

import java.util.Objects;

public final class ScenarioComparison {

  public String method;
  public Measure measure;
  public boolean showValue;
  public String referencePosition;

  public ScenarioComparison() {
  }

  public ScenarioComparison(String method,
                            Measure measure,
                            boolean showValue,
                            String referencePosition) {
    this.method = method;
    this.measure = measure;
    this.showValue = showValue;
    this.referencePosition = referencePosition;
  }

  public String method() {
    return this.method;
  }

  public Measure measure() {
    return this.measure;
  }

  public boolean showValue() {
    return this.showValue;
  }

  public String referencePosition() {
    return this.referencePosition;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (ScenarioComparison) obj;
    return Objects.equals(this.method, that.method) &&
            Objects.equals(this.measure, that.measure) &&
            this.showValue == that.showValue &&
            Objects.equals(this.referencePosition, that.referencePosition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.method, this.measure, this.showValue, this.referencePosition);
  }

  @Override
  public String toString() {
    return "ScenarioComparison[" +
            "method=" + this.method + ", " +
            "measure=" + this.measure + ", " +
            "showValue=" + this.showValue + ", " +
            "referencePosition=" + this.referencePosition + ']';
  }
}
