package io.squashql.query.compiled;

import io.squashql.query.Measure;
import io.squashql.query.VectorAggMeasure;
import io.squashql.query.database.QueryRewriter;
import io.squashql.type.TypedField;

public record CompiledVectorAggMeasure(VectorAggMeasure vectorAggMeasure,
                                       TypedField fieldToAggregate,
                                       TypedField vectorAxis) implements CompiledMeasure {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    throw new IllegalStateException("Incorrect path of execution");
  }

  @Override
  public String alias() {
    return measure().alias();
  }

  @Override
  public Measure measure() {
    return this.vectorAggMeasure;
  }

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
