package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.store.Field;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

import static me.paulbares.query.database.SqlUtils.escape;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class ExpressionMeasure implements Measure {

  public String alias;
  public String expression;

  public ExpressionMeasure(@NotNull String alias, @NotNull String expression) {
    this.alias = alias;
    this.expression = expression;
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

  @Override
  public void setExpression(String expression) {
    this.expression = expression;
  }
}
