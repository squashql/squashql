package io.squashql.query;

import io.squashql.query.database.QueryRewriter;
import io.squashql.type.TypedField;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class BinaryOperationField implements Field {

  public BinaryOperator operator;
  public Field leftOperand;
  public Field rightOperand;

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
    throw new IllegalStateException("Incorrect path of execution");
  }
}
