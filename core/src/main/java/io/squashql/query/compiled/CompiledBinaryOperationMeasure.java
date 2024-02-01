package io.squashql.query.compiled;

import io.squashql.query.BinaryOperator;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SqlUtils;

public record CompiledBinaryOperationMeasure(String alias,
                                             BinaryOperator operator,
                                             CompiledMeasure leftOperand,
                                             CompiledMeasure rightOperand) implements CompiledMeasure {
  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    String leftOperand = this.leftOperand.sqlExpression(queryRewriter, false);
    String rightOperand = this.rightOperand.sqlExpression(queryRewriter, false);
    String sql = queryRewriter.binaryOperation(this.operator, leftOperand, rightOperand);
    return withAlias ? SqlUtils.appendAlias(sql, queryRewriter, this.alias) : sql;
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
