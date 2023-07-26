package io.squashql.query.api;

import io.squashql.query.BinaryOperator;
import io.squashql.query.database.QueryRewriter;
import io.squashql.store.TypedField;
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

  @Override
  public String sqlExpression(Function<String, TypedField> fp, QueryRewriter qr) {
    return new StringBuilder()
            .append("(")
            .append(this.leftOperand.sqlExpression(fp, qr))
            .append(this.operator.infix)
            .append(this.rightOperand.sqlExpression(fp, qr))
            .append(")")
            .toString();
  }
}
