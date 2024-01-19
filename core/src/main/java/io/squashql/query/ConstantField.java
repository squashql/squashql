package io.squashql.query;

import io.squashql.type.TypedField;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class ConstantField implements CompilationType<TypedField> {

  public Object value;

  @Override
  public Field as(String alias) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public String alias() {
    throw new IllegalStateException("Incorrect path of execution");
  }
}
