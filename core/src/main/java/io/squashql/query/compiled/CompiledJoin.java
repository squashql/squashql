package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.dto.JoinType;

import java.util.function.Function;

public record CompiledJoin(NamedTable table, JoinType type, CompiledCriteria joinCriteria) {

  public String sqlExpression(QueryRewriter queryRewriter, Function<String, String> tableNameFunc) {
    StringBuilder statement = new StringBuilder();
    statement.append(" ")
            .append(this.type.name().toLowerCase())
            .append(" join ")
            .append(tableNameFunc.apply(this.table.name()))
            .append(" on ");
    statement.append(joinCriteria().sqlExpression(queryRewriter));

    if (!this.table.joins().isEmpty()) {
      this.table.joins().forEach(j -> statement.append(j.sqlExpression(queryRewriter, tableNameFunc)));
    }
    return statement.toString();
  }
}
