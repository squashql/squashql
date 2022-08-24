package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.store.Field;

import java.util.Objects;
import java.util.function.Function;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class UnresolvedExpressionMeasure implements Measure {

  public String alias;

  public UnresolvedExpressionMeasure(String alias) {
    this.alias = Objects.requireNonNull(alias);
  }

  @Override
  public String sqlExpression(Function<String, Field> fieldProvider, QueryRewriter queryRewriter) {
    throw new RuntimeException();
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public String expression() {
    throw new RuntimeException();
  }

  @Override
  public void setExpression(String expression) {
    throw new RuntimeException();
  }
}
