package io.squashql.type;

import io.squashql.query.BinaryOperator;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import static io.squashql.query.comp.BinaryOperations.getOutputType;


@EqualsAndHashCode
@AllArgsConstructor
@Getter
public class BinaryOperationTypedField implements TypedField {

  public BinaryOperator operator;
  public TypedField leftOperand;
  public TypedField rightOperand;
  public String alias;

  public BinaryOperationTypedField(BinaryOperator operator, TypedField leftOperand, TypedField rightOperand) {
    this(operator, leftOperand, rightOperand, null);
  }

  @Override
  public Class<?> type() {
    return getOutputType(this.operator, this.leftOperand.type(), this.rightOperand.type());
  }

  @Override
  public String alias() {
    return this.alias;
  }
}
