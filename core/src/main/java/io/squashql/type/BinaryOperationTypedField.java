package io.squashql.type;

import io.squashql.query.BinaryOperator;
import io.squashql.query.database.QueryRewriter;
import io.squashql.store.UnknownType;

public record BinaryOperationTypedField(BinaryOperator operator, TypedField leftOperand, TypedField rightOperand,
                                        String alias) implements TypedField {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter) {
    String leftOperand = this.leftOperand.sqlExpression(queryRewriter);
    String rightOperand = this.rightOperand.sqlExpression(queryRewriter);
    return queryRewriter.binaryOperation(this.operator, leftOperand, rightOperand);
  }

  @Override
  public Class<?> type() {
    return UnknownType.class;
  }

  @Override
  public String name() {
    throw new IllegalStateException("Incorrect path of execution");
  }

  @Override
  public TypedField as(String alias) {
    return new BinaryOperationTypedField(this.operator, this.leftOperand, this.rightOperand, alias);
  }
}
