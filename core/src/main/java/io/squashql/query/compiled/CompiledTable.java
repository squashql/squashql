package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;

import java.util.List;

public interface CompiledTable {

  List<CompiledJoin> joins();

  String sqlExpression(QueryRewriter queryRewriter);

  static <T extends CompiledTable & NamedTable> String sqlExpression(QueryRewriter queryRewriter, T table){
    StringBuilder statement = new StringBuilder();
    statement.append(table.sqlExpressionTableName(queryRewriter));
    List<CompiledJoin> joins = table.joins();
    if (joins != null) {
      joins.forEach(j -> statement.append(j.sqlExpression(queryRewriter)));
    }
    return statement.toString();
  }
}
