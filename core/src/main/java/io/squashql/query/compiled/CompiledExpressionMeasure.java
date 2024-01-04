package io.squashql.query.compiled;

import io.squashql.query.TotalCountMeasure;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SqlUtils;

public record CompiledExpressionMeasure(String alias, String expression) implements CompiledMeasure {

  /**
   * Represents a compiled total count measure.
   */
  public static final CompiledMeasure COMPILED_TOTAL_COUNT = new CompiledExpressionMeasure(TotalCountMeasure.ALIAS, TotalCountMeasure.EXPRESSION);


  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    return withAlias ? SqlUtils.appendAlias(this.expression, queryRewriter, alias()) : this.expression;
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
