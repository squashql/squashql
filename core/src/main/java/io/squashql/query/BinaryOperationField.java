package io.squashql.query;

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
  public String alias;

  public BinaryOperationField(BinaryOperator operator, Field leftOperand, Field rightOperand) {
    this.operator = operator;
    this.leftOperand = leftOperand;
    this.rightOperand = rightOperand;
  }

  @Override
  public Field as(String alias) {
    return new BinaryOperationField(this.operator, this.leftOperand, this.rightOperand, alias);
  }

  @Override
  public String alias() {
    return this.alias;
  }
}
