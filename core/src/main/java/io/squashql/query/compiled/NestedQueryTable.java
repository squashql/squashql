package io.squashql.query.compiled;

import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SQLTranslator;

import java.util.Collections;
import java.util.List;

public record NestedQueryTable(DatabaseQuery query) implements CompiledTable {

  @Override
  public List<CompiledJoin> joins() {
    return Collections.emptyList(); // not supported for the moment
  }

  @Override
  public String sqlExpression(QueryRewriter queryRewriter) {
    return "(" + SQLTranslator.translate(this.query, queryRewriter) + ")";
  }
}
