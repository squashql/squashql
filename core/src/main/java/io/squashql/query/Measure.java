package io.squashql.query;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Marker interface.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface Measure {

  String alias();

  String expression();

  Measure withExpression(String expression);

  <R> R accept(MeasureVisitor<R> visitor);
}
