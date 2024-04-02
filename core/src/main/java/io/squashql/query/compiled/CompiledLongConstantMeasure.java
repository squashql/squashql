package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;

public record CompiledLongConstantMeasure(Long value) implements CompiledMeasure {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    return String.valueOf(this.value);
  }

  @Override
  public String alias() {
    return "constant(" + this.value + ")";
  }

  @Override
  public <R> R accept(CompiledMeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
