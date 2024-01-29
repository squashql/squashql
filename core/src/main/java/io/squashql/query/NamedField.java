package io.squashql.query;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface NamedField extends Field {

  String name();

  NamedField as(String alias);

}
