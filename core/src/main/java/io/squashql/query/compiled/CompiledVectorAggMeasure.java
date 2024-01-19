package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;
import io.squashql.type.NamedTypedField;

public record CompiledVectorAggMeasure(String alias,
                                       NamedTypedField fieldToAggregate,
                                       String aggregationFunction,
                                       NamedTypedField vectorAxis) implements CompiledMeasure {

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
