package io.squashql.query.compiled;

import io.squashql.query.Measure;
import io.squashql.query.database.QueryRewriter;

public interface CompiledMeasure {

  String sqlExpression(QueryRewriter queryRewriter, boolean withAlias);

  String alias();

  Measure measure();

  <R> R accept(MeasureVisitor<R> visitor);

}
