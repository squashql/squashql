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

  public String function;
  public Field field;
  public String alias;

  public FunctionField(String function) {
    this(function, null);
  }

  public FunctionField(String function, Field field) {
    this.function = function;
    this.field = field;
  }

  @Override
  public Field as(String alias) {
    return new FunctionField(this.function, this.field, alias);
  }

  @Override
  public String alias() {
    return this.alias;
  }
}
