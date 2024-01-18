package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;

import java.util.List;

public record MaterializedTable(String name, List<CompiledJoin> joins) implements CompiledTable, NamedTable {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter) {
    return CompiledTable.sqlExpression(queryRewriter, this);
  }

  @Override
  public String sqlExpressionTableName(QueryRewriter queryRewriter) {
    return queryRewriter.tableName(this.name);
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
