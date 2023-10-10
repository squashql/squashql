package io.squashql.type;

import io.squashql.query.BinaryOperator;
import io.squashql.query.database.QueryRewriter;

public record BinaryOperationTypedField(BinaryOperator operator, TypedField leftOperand, TypedField rightOperand) implements TypedField {
  @Override
  public String sqlExpression(QueryRewriter queryRewriter) {
    return new StringBuilder()
            .append("(")
            .append(this.leftOperand.sqlExpression(queryRewriter))
            .append(this.operator.infix)
            .append(this.rightOperand.sqlExpression(queryRewriter))
            .append(")")
            .toString();
  }

  @Override
  public Class<?> type() {
    // todo should we resolve based on the operand types ?
    return null;
  }
}
