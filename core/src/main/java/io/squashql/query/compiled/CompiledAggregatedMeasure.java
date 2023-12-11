package io.squashql.query.compiled;

import io.squashql.query.AggregatedMeasure;
import io.squashql.query.CountMeasure;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SqlUtils;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;

public record CompiledAggregatedMeasure(AggregatedMeasure measure, TypedField field,
                                        CompiledCriteria criteria) implements CompiledMeasure {

  public static final CompiledMeasure COMPILED_COUNT = new CompiledAggregatedMeasure(
          CountMeasure.INSTANCE, new TableTypedField(null, CountMeasure.FIELD_NAME, long.class), null);

  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    String sql;
    String fieldExpression = this.field.sqlExpression(queryRewriter);
    if (this.criteria != null) {
      sql = this.measure.aggregationFunction + "(case when " + this.criteria.sqlExpression(queryRewriter) + " then " + fieldExpression + " end)";
    } else {
      sql = this.measure.aggregationFunction + "(" + fieldExpression + ")";
    }
    if (measure.distinct) {
      sql = "distinct(" + sql + ")";
    }
    return withAlias ? SqlUtils.appendAlias(sql, queryRewriter, alias()) : sql;
  }

  @Override
  public String alias() {
    return measure().alias();
  }

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }

}
