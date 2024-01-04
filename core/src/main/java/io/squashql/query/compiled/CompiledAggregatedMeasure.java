package io.squashql.query.compiled;

import io.squashql.query.CountMeasure;
import io.squashql.query.agg.AggregationFunction;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SqlUtils;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;

public record CompiledAggregatedMeasure(String alias,
                                        TypedField field,
                                        String aggregationFunction,
                                        CompiledCriteria criteria,
                                        boolean distinct) implements CompiledMeasure {

  public static final CompiledMeasure COMPILED_COUNT = new CompiledAggregatedMeasure(
          CountMeasure.INSTANCE.alias, new TableTypedField(null, CountMeasure.FIELD_NAME, long.class), AggregationFunction.COUNT, null, false);

  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    final String fieldExpression = this.field.sqlExpression(queryRewriter);
    String valuesToAggregate;
    if (this.criteria != null) {
      valuesToAggregate = "case when " + this.criteria.sqlExpression(queryRewriter) + " then " + fieldExpression + " end";
    } else {
      valuesToAggregate = fieldExpression;
    }
    if (this.distinct) {
      valuesToAggregate = "distinct(" + valuesToAggregate + ")";
    }
    final String sql = this.aggregationFunction + "(" + valuesToAggregate + ")";
    return withAlias ? SqlUtils.appendAlias(sql, queryRewriter, alias()) : sql;
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
