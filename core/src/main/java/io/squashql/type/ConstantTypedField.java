package io.squashql.type;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;


@EqualsAndHashCode
@AllArgsConstructor
@Getter
public class ConstantTypedField implements TypedField {

  public final Object value;
  public final String alias;

  public ConstantTypedField(Object value) {
    this(value, null);
  }

  @Override
  public Class<?> type() {
    return this.value.getClass();
  }

  @Override
  public String alias() {
    return this.alias;
  }
}
