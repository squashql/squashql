package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;

public record CompiledLongConstantMeasure(Long value, String alias) implements CompiledMeasure {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    return Long.toString(this.value);
  }
}
