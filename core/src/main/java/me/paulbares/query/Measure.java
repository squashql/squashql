package me.paulbares.query;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Marker interface.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface Measure {

  String sqlExpression();

  String alias();
}
