package io.squashql.query;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.squashql.query.database.QueryRewriter;
import io.squashql.type.TypedField;

import java.util.function.Function;

/**
 * Marker interface.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface Measure {

  String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias);

  String alias();

  String expression();

  Measure withExpression(String expression);

  <R> R accept(MeasureVisitor<R> visitor);
}
