package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.store.TypedField;

import java.util.function.Function;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class UnresolvedExpressionMeasure implements Measure {

  public String alias;

  public UnresolvedExpressionMeasure(@NonNull String alias) {
    this.alias = alias;
  }

  @Override
  public String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
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

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
