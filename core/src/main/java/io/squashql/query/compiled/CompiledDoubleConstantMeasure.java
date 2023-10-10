package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;

public record CompiledDoubleConstantMeasure(Double value, String alias) implements CompiledMeasure {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    return Double.toString(this.value);
  }
}
