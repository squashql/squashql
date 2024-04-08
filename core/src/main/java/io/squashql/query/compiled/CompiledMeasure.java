package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;

public interface CompiledMeasure {

  String sqlExpression(QueryRewriter queryRewriter, boolean withAlias);

  String alias();

  <R> R accept(CompiledMeasureVisitor<R> visitor);
}
