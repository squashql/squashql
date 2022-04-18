package me.paulbares.query.dto;

import me.paulbares.query.Measure;

import java.util.Objects;

public final class ScenarioComparisonDto {

  public String method;
  public Measure measure;
  public boolean showValue;
  public String referencePosition;
  public String label;

  public ScenarioComparisonDto() {
  }

  public ScenarioComparisonDto(String method,
                               Measure measure,
                               boolean showValue,
                               String referencePosition) {
    this(method, measure, showValue, referencePosition, null);
  }

  public ScenarioComparisonDto(String method,
                               Measure measure,
                               boolean showValue,
                               String referencePosition,
                               String label) {
    this.method = method;
    this.measure = measure;
    this.showValue = showValue;
    this.referencePosition = referencePosition;
    this.label = label;
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

  public String label() {
    return this.label;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (ScenarioComparisonDto) obj;
    return Objects.equals(this.method, that.method) &&
            Objects.equals(this.measure, that.measure) &&
            this.showValue == that.showValue &&
            Objects.equals(this.referencePosition, that.referencePosition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.method, this.measure, this.showValue, this.referencePosition, this.label);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[" +
            "method=" + this.method + ", " +
            "measure=" + this.measure + ", " +
            "showValue=" + this.showValue + ", " +
            "referencePosition=" + this.referencePosition + ", " +
            "label=" + this.label + ']';
  }
}
