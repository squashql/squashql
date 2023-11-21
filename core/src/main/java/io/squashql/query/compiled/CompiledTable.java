package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.VirtualTableDto;

import java.util.List;
import java.util.function.Function;

public record CompiledTable(String name, List<CompiledJoin> joins) {

  public String sqlExpression(final QueryRewriter queryRewriter, final VirtualTableDto virtualTable) {
    final Function<String, String> tableNameFunc = tableName -> virtualTable != null && virtualTable.name.equals(tableName)
            ? queryRewriter.cteName(tableName) : queryRewriter.tableName(tableName);
    final StringBuilder statement = new StringBuilder();
    this.joins.forEach(j -> statement.append(j.sqlExpression(queryRewriter, tableNameFunc)));
    return statement.toString();
  }

  public record CompiledJoin(CompiledTable table, JoinType type, CompiledCriteria joinCriteria) {
    String sqlExpression(final QueryRewriter queryRewriter, final Function<String, String> tableNameFunc) {
      final StringBuilder statement = new StringBuilder();
      statement.append(" ")
              .append(this.type.name().toLowerCase())
              .append(" join ")
              .append(tableNameFunc.apply(this.table.name()))
              .append(" on ");
      statement.append(joinCriteria().sqlExpression(queryRewriter));

      if (!this.table.joins().isEmpty()) {
        this.table.joins.forEach(j -> statement.append(j.sqlExpression(queryRewriter, tableNameFunc)));
      }
      return statement.toString();
    }
  }
}
