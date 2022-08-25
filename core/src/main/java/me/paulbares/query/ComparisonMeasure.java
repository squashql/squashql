package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.store.Field;
import org.jetbrains.annotations.NotNull;

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

  public ComparisonMeasure(@NotNull String alias,
                           @NotNull ComparisonMethod method,
                           @NotNull Measure measure,
                           @NotNull String columnSet,
                           @NotNull Map<String, String> referencePosition) {
    this.alias = alias;
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
    return this.expression;
  }

  @Override
  public void setExpression(String expression) {
    this.expression = expression;
  }
}
