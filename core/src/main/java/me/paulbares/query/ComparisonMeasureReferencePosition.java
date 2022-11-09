package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.query.dto.Period;
import me.paulbares.store.Field;

import java.util.List;
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
  public Period period;
  public List<String> ancestors;

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull Map<String, String> referencePosition,
                                            @NonNull Period period) {
    this(alias, comparisonMethod, measure, referencePosition, period, null, null);
  }

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull Map<String, String> referencePosition,
                                            @NonNull ColumnSetKey columnSetKey) {
    this(alias, comparisonMethod, measure, referencePosition, null, columnSetKey, null);
  }

  public ComparisonMeasureReferencePosition(@NonNull String alias,
                                            @NonNull ComparisonMethod comparisonMethod,
                                            @NonNull Measure measure,
                                            @NonNull List<String> ancestors) {
    this(alias, comparisonMethod, measure, null, null, null, ancestors);
  }

  private ComparisonMeasureReferencePosition(@NonNull String alias,
                                             @NonNull ComparisonMethod comparisonMethod,
                                             @NonNull Measure measure,
                                             Map<String, String> referencePosition,
                                             Period period,
                                             ColumnSetKey columnSetKey,
                                             List<String> ancestors) {
    this.alias = alias;
    this.comparisonMethod = comparisonMethod;
    this.measure = measure;
    this.referencePosition = referencePosition;
    this.period = period;
    this.columnSetKey = columnSetKey;
    this.ancestors = ancestors;
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
  public String sqlExpression(Function<String, Field> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
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
