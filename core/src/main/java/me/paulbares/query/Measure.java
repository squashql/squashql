package me.paulbares.query;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.store.TypedField;

import java.util.function.Function;

/**
 * Marker interface.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface Measure {

  String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias);

  String alias();

  String expression();

  void setExpression(String expression);

  <R> R accept(MeasureVisitor<R> visitor);
}
