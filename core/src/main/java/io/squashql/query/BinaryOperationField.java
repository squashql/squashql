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

  @Override
  public String name() {
    throw new IllegalStateException("Incorrect path of execution");
  }
}
