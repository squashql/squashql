package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.store.TypedField;

import java.util.Map;
import java.util.function.Function;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class ComparisonMeasureReferencePosition implements Measure, ComparisonMeasure {

  public String alias;
  public String expression;
  public ComparisonMethod comparisonMethod;
  public Measure measure;
  public ColumnSetKey columnSetKey;
  public Map<String, String> referencePosition;

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull ColumnSetKey columnSetKey,
                                            @NonNull Map<String, String> referencePosition) {
    this.alias = alias;
    this.comparisonMethod = comparisonMethod;
    this.columnSetKey = columnSetKey;
    this.measure = measure;
    this.referencePosition = referencePosition;
  }

  @Override
  public ColumnSetKey getColumnSetKey() {
    return this.columnSetKey;
  }

  @Override
  public Measure getMeasure() {
    return this.measure;
  }

  @Override
  public ComparisonMethod getComparisonMethod() {
    return this.comparisonMethod;
  }

  @Override
  public String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
    throw new IllegalStateException();
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public String expression() {
    return this.expression;
  }

  @Override
  public void setExpression(String expression) {
    this.expression = expression;
  }

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
