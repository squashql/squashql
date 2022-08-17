package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.store.Field;

import java.util.Objects;
import java.util.function.Function;

import static me.paulbares.query.database.SqlUtils.escape;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class ExpressionMeasure implements Measure {

  public String alias;
  public String expression;

  public ExpressionMeasure(String alias, String expression) {
    this.alias = Objects.requireNonNull(alias);
    this.expression = Objects.requireNonNull(expression);
  }

  @Override
  public String sqlExpression(Function<String, Field> fieldProvider, QueryRewriter queryRewriter) {
    return this.expression + " as " + escape(this.alias);
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public String expression() {
    return this.expression;
  }
}
