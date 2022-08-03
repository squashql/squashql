package me.paulbares.query;

import me.paulbares.query.database.QueryRewriter;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Field;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class ComparisonMeasure implements Measure {

  public String alias;
  public ComparisonMethod method;
  public Measure measure;
  public String columnSet;
  public Map<String, String> referencePosition;

  /**
   * For jackson.
   */
  public ComparisonMeasure() {
  }

  public ComparisonMeasure(String alias,
                           ComparisonMethod method,
                           Measure measure,
                           String columnSet,
                           Map<String, String> referencePosition) {
    this.alias = alias == null
            ? String.format("%s(%s, %s)", method, measure.alias(), referencePosition)
            : alias;
    this.method = method;
    this.columnSet = columnSet;
    this.measure = measure;
    this.referencePosition = referencePosition;
  }

  @Override
  public String sqlExpression(Function<String, Field> fieldProvider, QueryRewriter queryRewriter) {
    throw new IllegalStateException();
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public String expression() {
    String alias = this.measure.alias();
    if (this.columnSet.equals(QueryDto.PERIOD)) {
      String formula = this.method.expressionGenerator.apply(alias + "(current period)", alias + "(reference period)");
      return formula + ", reference = " + this.referencePosition;
    } else if (this.columnSet.equals(QueryDto.BUCKET)) {
      String formula = this.method.expressionGenerator.apply(alias + "(current bucket)", alias + "(reference bucket)");
      return formula + ", reference = " + this.referencePosition;
    } else {
      return "";
    }
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
            && this.columnSet.equals(that.columnSet)
            && this.referencePosition.equals(that.referencePosition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.alias, this.method, this.measure, this.columnSet, this.referencePosition);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
            + '{' +
            "alias='" + this.alias + '\'' +
            ", method='" + this.method + '\'' +
            ", measure=" + this.measure +
            ", columnSet=" + this.columnSet +
            ", referencePosition=" + this.referencePosition +
            '}';
  }
}
