package io.squashql.type;

import io.squashql.store.UnknownType;

public record FunctionTypedField(TableTypedField field, String function) implements TypedField {

  @Override
  public Class<?> type() {
    return UnknownType.class;
  }
}
