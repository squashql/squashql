package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SqlUtils;
import io.squashql.type.TypedField;

public record CompiledAggregatedMeasure(String alias, TypedField field, String aggregationFunction,
                                        CompiledCriteria criteria) implements CompiledMeasure {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    String sql;
    String fieldExpression = this.field.sqlExpression(queryRewriter);
    if (this.criteria != null) {
      sql = this.aggregationFunction + "(case when " + this.criteria.sqlExpression(queryRewriter) + " then " + fieldExpression + " end)";
    } else {
      sql = this.aggregationFunction + "(" + fieldExpression + ")";
    }
    return withAlias ? SqlUtils.appendAlias(sql, queryRewriter, this.alias) : sql;
  }

}
