package io.squashql.query.compiled;

import io.squashql.query.ConstantMeasure;
import io.squashql.query.database.QueryRewriter;

public record CompiledConstantMeasure(ConstantMeasure<?> measure) implements CompiledMeasure {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    return this.measure.getValue().toString();
  }

  @Override
  public String alias() {
    return this.measure.alias();
  }

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }

}
