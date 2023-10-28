package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SqlUtils;

public record CompiledExpressionMeasure(String expression, String alias) implements CompiledMeasure {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    return withAlias ? SqlUtils.appendAlias(this.expression, queryRewriter, this.alias) : this.expression;
  }

  @Override
  public String alias() {
    return null;
  }

}
