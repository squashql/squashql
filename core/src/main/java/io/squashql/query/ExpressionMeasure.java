package io.squashql.query;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SqlUtils;
import io.squashql.type.TypedField;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.With;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class ExpressionMeasure implements BasicMeasure {

  public String alias;
  @With
  public String expression;

  @Override
  public String sqlExpression(Function<Field, TypedField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
    return withAlias ? SqlUtils.appendAlias(this.expression, queryRewriter, this.alias) : this.expression;
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
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
