package io.squashql.query.field;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface Field {

  Field as(String alias);

  String alias();
}
