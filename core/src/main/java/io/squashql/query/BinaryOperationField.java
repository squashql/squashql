package io.squashql.query;

import io.squashql.query.database.QueryRewriter;
import io.squashql.type.TypedField;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.function.Function;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class BinaryOperationField implements Field {

  public BinaryOperator operator;
  public Field leftOperand;
  public Field rightOperand;
  public String alias;

  public BinaryOperationField(BinaryOperator operator, Field leftOperand, Field rightOperand) {
    this(operator, leftOperand, rightOperand, null);
  }

  @Override
  public String sqlExpression(Function<Field, TypedField> fp, QueryRewriter qr) {
    return new StringBuilder()
            .append("(")
            .append(this.leftOperand.sqlExpression(fp, qr))
            .append(this.operator.infix)
            .append(this.rightOperand.sqlExpression(fp, qr))
            .append(")")
            .toString();
  }

  @Override
  public String name() {
    return new StringBuilder()
            .append("(")
            .append(this.leftOperand.name())
            .append(this.operator.infix)
            .append(this.rightOperand.name())
            .append(")")
            .toString();
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public Field as(String alias) {
    return new BinaryOperationField(this.operator, this.leftOperand, this.rightOperand, alias);
  }
}
