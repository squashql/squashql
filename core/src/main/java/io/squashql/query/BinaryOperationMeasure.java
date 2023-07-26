package io.squashql.query;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SqlUtils;
import io.squashql.store.TypedField;
import lombok.*;

import java.util.function.Function;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class BinaryOperationMeasure implements Measure {

  public String alias;
  @With
  public String expression;
  public BinaryOperator operator;
  public Measure leftOperand;
  public Measure rightOperand;

  public BinaryOperationMeasure(@NonNull String alias,
                                @NonNull BinaryOperator binaryOperator,
                                @NonNull Measure leftOperand,
                                @NonNull Measure rightOperand) {
    this.alias = alias;
    this.operator = binaryOperator;
    this.leftOperand = leftOperand;
    this.rightOperand = rightOperand;
  }

  @Override
  public String sqlExpression(Function<String, TypedField> fp, QueryRewriter qr, boolean withAlias) {
    String sql = new StringBuilder()
            .append("(")
            .append(this.leftOperand.sqlExpression(fp, qr, false))
            .append(this.operator.infix)
            .append(this.rightOperand.sqlExpression(fp, qr, false))
            .append(")")
            .toString();
    return withAlias ? SqlUtils.appendAlias(sql, qr, this.alias) : sql;
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public String expression() {
    return this.expression;
  }

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
