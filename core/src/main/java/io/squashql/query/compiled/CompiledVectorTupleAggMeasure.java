package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;
import io.squashql.type.TypedField;

import java.util.List;
import java.util.function.Function;

public record CompiledVectorTupleAggMeasure(String alias,
                                            List<CompiledFieldAndAggFunc> fieldToAggregateAndAggFunc,
                                            TypedField vectorAxis,
                                            Function<List<Object>, Object> transformer) implements CompiledMeasure {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    throw new IllegalStateException("Incorrect path of execution");
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
