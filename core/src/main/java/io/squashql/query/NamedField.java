package io.squashql.query;

public interface NamedField extends Field {

  String name();

  NamedField as(String alias);

}
