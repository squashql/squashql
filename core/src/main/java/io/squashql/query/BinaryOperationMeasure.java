package io.squashql.query;

import lombok.*;

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
