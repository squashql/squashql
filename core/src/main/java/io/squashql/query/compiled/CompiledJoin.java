package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.dto.JoinType;

public record CompiledJoin(NamedTable table, JoinType type, CompiledCriteria joinCriteria) {

  public String sqlExpression(QueryRewriter queryRewriter) {
    StringBuilder statement = new StringBuilder()
            .append(" ")
            .append(this.type.name().toLowerCase())
            .append(" join ")
            .append(this.table.sqlExpressionTableName(queryRewriter));
    if (joinCriteria() != null) {
      statement
              .append(" on ")
              .append(joinCriteria().sqlExpression(queryRewriter));
    }

    if (!this.table.joins().isEmpty()) {
      this.table.joins().forEach(j -> statement.append(j.sqlExpression(queryRewriter)));
    }
    return statement.toString();
  }
}
