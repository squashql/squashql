package me.paulbares.query.context;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface ContextValue {

  /**
   * Returns the key of the context value used to uniquely identify it.
   *
   * @return the key.
   */
  String key();
}
