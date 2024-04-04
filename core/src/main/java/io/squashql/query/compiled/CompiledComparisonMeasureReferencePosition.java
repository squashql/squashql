package io.squashql.query.compiled;

import io.squashql.query.ColumnSetKey;
import io.squashql.query.ComparisonMethod;
import io.squashql.query.database.QueryRewriter;
import io.squashql.type.TypedField;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public record CompiledComparisonMeasureReferencePosition(String alias,
                                                         ComparisonMethod comparisonMethod,
                                                         BiFunction<Object, Object, Object> comparisonOperator,
                                                         CompiledMeasure measure,
                                                         Map<TypedField, String> referencePosition,
                                                         CompiledPeriod period,
                                                         ColumnSetKey columnSetKey,
                                                         List<?> elements,
                                                         List<TypedField> ancestors,
                                                         boolean grandTotalAlongAncestors) implements CompiledComparisonMeasure {


  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    throw new IllegalStateException("incorrect path of execution");
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
