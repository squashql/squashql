package io.squashql.query;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.squashql.query.database.QueryRewriter;
import io.squashql.store.Field;

import java.util.function.Function;

/**
 * Marker interface.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface Measure {

  String sqlExpression(Function<String, Field> fieldProvider, QueryRewriter queryRewriter, boolean withAlias);

  String alias();

  String expression();

  void setExpression(String expression);

  <R> R accept(MeasureVisitor<R> visitor);
}
