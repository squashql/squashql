package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;

import java.util.List;

public record CteTable(String name,
                       List<CompiledJoin> joins) implements CompiledTable, NamedTable {

  @Override
  public String sqlExpressionTableName(QueryRewriter queryRewriter) {
    return queryRewriter.cteName(this.name);
  }

  @Override
  public String sqlExpression(QueryRewriter queryRewriter) {
    return CompiledTable.sqlExpression(queryRewriter, this);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("CteTable{");
    sb.append("name='").append(this.name).append('\'');
    if (this.joins != null && !this.joins.isEmpty()) {
      sb.append(", joins=").append(this.joins);
    }
    sb.append('}');
    return sb.toString();
  }
}
