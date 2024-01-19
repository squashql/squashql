package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public record CteRecordTable(String name,
                             List<String> fields,
                             List<List<Object>> records) implements CompiledTable, NamedTable {

  @Override
  public String sqlExpressionTableName(QueryRewriter queryRewriter) {
    return queryRewriter.cteName(this.name);
  }

  @Override
  public List<CompiledJoin> joins() {
    return Collections.emptyList(); // FIXME not sure about this one. Might be chained with other cte??
  }

  @Override
  public String sqlExpression(QueryRewriter qr) {
    StringBuilder statement = new StringBuilder();
    statement.append(qr.cteName(this.name)).append(" as (");
    Iterator<List<Object>> it = this.records.iterator();
    while (it.hasNext()) {
      statement.append("select ");
      List<Object> row = it.next();
      for (int i = 0; i < row.size(); i++) {
        Object obj = row.get(i);
        statement.append(obj instanceof String ? "'" : "");
        statement.append(obj);
        statement.append(obj instanceof String ? "'" : "");
        statement.append(" as ").append(qr.fieldName(this.fields.get(i)));
        if (i < row.size() - 1) {
          statement.append(", ");
        }
      }
      if (it.hasNext()) {
        statement.append(" union all ");
      }
    }
    return statement.append(")").toString();
  }
}
