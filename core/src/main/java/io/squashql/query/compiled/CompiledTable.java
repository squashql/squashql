package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.VirtualTableDto;

import java.util.List;
import java.util.function.Function;

public record CompiledTable(String name, List<CompiledJoin> joins) {

  public String sqlExpression(QueryRewriter queryRewriter) {
    StringBuilder statement = new StringBuilder();
    List<VirtualTableDto> virtualTables = queryRewriter.query() != null ? queryRewriter.query().virtualTableDtos : null;
    statement.append(queryRewriter.tableName(this.name));
    Function<String, String> tableNameFunc = tableName -> {
      if (virtualTables != null) {
        for (VirtualTableDto virtualTable : virtualTables) {
          if (virtualTable.name.equals(tableName)) {
            return queryRewriter.cteName(tableName);
          }
        }
      }
      return queryRewriter.tableName(tableName);
    };
    this.joins.forEach(j -> statement.append(j.sqlExpression(queryRewriter, tableNameFunc)));
    return statement.toString();
  }

  public record CompiledJoin(CompiledTable table, JoinType type, CompiledCriteria joinCriteria) {
    public String sqlExpression(QueryRewriter queryRewriter, Function<String, String> tableNameFunc) {
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
    sb.append("name='").append(this.name).append('\'');
    if (this.joins != null && !joins().isEmpty()) {
      sb.append(", joins=").append(this.joins);
    }
    sb.append('}');
    return sb.toString();
  }
}
