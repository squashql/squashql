package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.VirtualTableDto;

import java.util.List;
import java.util.function.Function;

public record CompiledTable(String name, List<CompiledJoin> joins) {

  public String sqlExpression(QueryRewriter queryRewriter) {
    StringBuilder statement = new StringBuilder();
    VirtualTableDto virtualTable = queryRewriter.query() != null ? queryRewriter.query().virtualTableDto : null;
    statement.append(queryRewriter.tableName(this.name));
    Function<String, String> tableNameFunc = tableName -> virtualTable != null && virtualTable.name.equals(tableName)
            ? queryRewriter.cteName(tableName) : queryRewriter.tableName(tableName);
    this.joins.forEach(j -> statement.append(j.sqlExpression(queryRewriter, tableNameFunc)));
    return statement.toString();
  }

  public record CompiledJoin(CompiledTable table, JoinType type, CompiledCriteria joinCriteria) {
    String sqlExpression(QueryRewriter queryRewriter, Function<String, String> tableNameFunc) {
      StringBuilder statement = new StringBuilder();
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

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("CompiledTable{");
    sb.append("name='").append(name).append('\'');
    if (joins != null && !joins().isEmpty()) {
      sb.append(", joins=").append(joins);
    }
    sb.append('}');
    return sb.toString();
  }
}
