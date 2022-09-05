package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.store.Field;

import java.util.Map;
import java.util.function.Function;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class ComparisonMeasure implements Measure {

  public String alias;
  public String expression;
  public ComparisonMethod method;
  public Measure measure;
  public String columnSet;
  public Map<String, String> referencePosition;

  public ComparisonMeasure(@NonNull String alias,
                           @NonNull ComparisonMethod method,
                           @NonNull Measure measure,
                           @NonNull String columnSet,
                           @NonNull Map<String, String> referencePosition) {
    this.alias = alias;
    this.method = method;
    this.columnSet = columnSet;
    this.measure = measure;
    this.referencePosition = referencePosition;
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
