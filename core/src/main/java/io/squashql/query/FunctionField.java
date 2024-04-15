package io.squashql.query;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class FunctionField implements Field {

  public String functionName;
  public Field operand;
  public String alias;

  public FunctionField(String functionName) {
    this(functionName, null);
  }

  public FunctionField(String functionName, Field operand) {
    this.functionName = functionName;
    this.operand = operand;
  }

  @Override
  public Field as(String alias) {
    return new FunctionField(this.functionName, this.operand, alias);
  }

  @Override
  public String alias() {
    return this.alias;
  }
}
