package io.squashql.query.parameter;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface Parameter {

  /**
   * Returns the key of the parameter used to uniquely identify it.
   *
   * @return the key.
   */
  String key();
}
