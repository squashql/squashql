package io.squashql.type;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.squashql.query.database.QueryRewriter;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface TypedField {

  String sqlExpression(QueryRewriter queryRewriter);

  Class<?> type();

  String alias();

  TypedField as(String alias);
}
