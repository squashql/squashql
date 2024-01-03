package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;
import io.squashql.type.TypedField;

public record CompiledVectorAggMeasure(String alias,
                                       TypedField fieldToAggregate,
                                       String aggregationFunction,
                                       TypedField vectorAxis) implements CompiledMeasure {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    throw new IllegalStateException("Incorrect path of execution");
  }

  @Override
  public String alias() {
    return this.alias;
  }

//  @Override
//  public Measure measure() {
//    return this.vectorAggMeasure;
//  }

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
