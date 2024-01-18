package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;

import java.util.List;
import java.util.function.Function;

public record MaterializedTable(String name, List<CompiledJoin> joins) implements CompiledTable, NamedTable {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter) {
    StringBuilder statement = new StringBuilder();
    List<CteRecordTable> virtualTables = queryRewriter.query() != null ? queryRewriter.query().cteRecordTables : null;
    statement.append(queryRewriter.tableName(this.name));
    Function<String, String> tableNameFunc = tableName -> {
      if (virtualTables != null) {
        for (CteRecordTable virtualTable : virtualTables) {
          if (virtualTable.name().equals(tableName)) {
            return queryRewriter.cteName(tableName);
          }
        }
      }
      return queryRewriter.tableName(tableName);
    };
    this.joins.forEach(j -> statement.append(j.sqlExpression(queryRewriter, tableNameFunc)));
    return statement.toString();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("MaterializedTable{");
    sb.append("name='").append(this.name).append('\'');
    if (this.joins != null && !this.joins.isEmpty()) {
      sb.append(", joins=").append(this.joins);
    }
    sb.append('}');
    return sb.toString();
  }
}
