package io.squashql.query;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import static io.squashql.query.database.SqlUtils.singleOperandFunctionName;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class FunctionField implements NamedField {

  public String function;
  public NamedField field;
  public String alias;

  public FunctionField(String function, NamedField field) {
    this.function = function;
    this.field = field;
  }

  @Override
  public String name() {
    return singleOperandFunctionName(this.function, this.field.name());
  }

  @Override
  public NamedField as(String alias) {
    return new FunctionField(this.function, this.field, alias);
  }

  @Override
  public String alias() {
    return this.alias;
  }
}
