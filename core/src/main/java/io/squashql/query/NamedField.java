package io.squashql.query;

import io.squashql.type.NamedTypedField;

public interface NamedField extends CompilationType<NamedTypedField> {

  String name();

  NamedField as(String alias);

}
