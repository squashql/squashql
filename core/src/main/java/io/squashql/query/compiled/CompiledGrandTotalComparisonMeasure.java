package io.squashql.query.compiled;

import io.squashql.query.ComparisonMethod;
import io.squashql.query.database.QueryRewriter;

import java.util.function.BiFunction;

public record CompiledGrandTotalComparisonMeasure(String alias,
                                                  ComparisonMethod comparisonMethod,
                                                  CompiledMeasure measure) implements CompiledComparisonMeasure {

  @Override
  public BiFunction<Object, Object, Object> comparisonOperator() {
    return null;
  }

  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    throw new IllegalStateException("incorrect path of execution");
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public <R> R accept(CompiledMeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
