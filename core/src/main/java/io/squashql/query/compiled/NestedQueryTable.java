package io.squashql.query.compiled;

import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SqlTranslator;

import java.util.List;

public record NestedQueryTable(DatabaseQuery query, List<CompiledJoin> joins) implements CompiledTable {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter) {
    StringBuilder statement = new StringBuilder();
    statement.append("(").append(SqlTranslator.translate(this.query, queryRewriter)).append(")");
    if (this.joins != null) {
      this.joins.forEach(j -> statement.append(j.sqlExpression(queryRewriter)));
    }
    return statement.toString();
  }
}
