package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.store.Field;

import java.util.List;
import java.util.function.Function;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class ParentComparisonMeasure implements Measure, ComparisonMeasure {

  public String alias;
  public String expression;
  public ComparisonMethod method;
  public Measure measure;
  public ColumnSetKey columnSetKey;
  public List<String> ancestors;

  public ParentComparisonMeasure(@NonNull String alias,
                                 @NonNull ComparisonMethod method,
                                 @NonNull Measure measure,
                                 @NonNull List<String> ancestors) {
    this.alias = alias;
    this.method = method;
    this.measure = measure;
    this.ancestors = ancestors;
  }

  @Override
  public ColumnSetKey getColumnSetKey() {
    return ColumnSetKey.PARENT;
  }

  @Override
  public Measure getMeasure() {
    return this.measure;
  }

  @Override
  public ComparisonMethod getComparisonMethod() {
    return this.method;
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
}
