package io.squashql.query.compiled;

import io.squashql.query.BinaryOperationMeasure;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SqlUtils;

public record CompiledBinaryOperationMeasure(BinaryOperationMeasure measure,
                                             CompiledMeasure leftOperand,
                                             CompiledMeasure rightOperand) implements CompiledMeasure {
  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    String sql = new StringBuilder()
            .append("(")
            .append(this.leftOperand.sqlExpression(queryRewriter, false))
            .append(this.measure.operator.infix)
            .append(this.rightOperand.sqlExpression(queryRewriter, false))
            .append(")")
            .toString();
    return withAlias ? SqlUtils.appendAlias(sql, queryRewriter, this.measure.alias) : sql;
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
