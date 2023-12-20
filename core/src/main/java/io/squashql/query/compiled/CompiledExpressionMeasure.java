package io.squashql.query.compiled;

import io.squashql.query.Measure;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SqlUtils;

public record CompiledExpressionMeasure(Measure measure) implements CompiledMeasure {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    return withAlias ? SqlUtils.appendAlias(measure().expression(), queryRewriter, alias()) : measure.expression();
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
