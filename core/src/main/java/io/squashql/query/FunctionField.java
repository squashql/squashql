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
public class FunctionField implements Field {

  public String function;
  public Field field;

  @Override
  public String name() {
    return singleOperandFunctionName(this.function, this.field.name());
  }
}
