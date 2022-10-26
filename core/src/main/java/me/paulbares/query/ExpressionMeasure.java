package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.query.database.SqlUtils;
import me.paulbares.store.TypedField;

import java.util.function.Function;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class ExpressionMeasure implements Measure {

  public String alias;
  public String expression;

  public ExpressionMeasure(@NonNull String alias, @NonNull String expression) {
    this.alias = alias;
    this.expression = expression;
  }

  @Override
  public String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
    return withAlias ? SqlUtils.appendAlias(this.expression, queryRewriter, this.alias, this) : this.expression;
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
