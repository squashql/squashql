package io.squashql.type;

public interface NamedTypedField extends TypedField {

  String name();

  @Override
  NamedTypedField as(String alias);

}
