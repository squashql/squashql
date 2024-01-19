package io.squashql.type;

import io.squashql.query.database.QueryRewriter;

public interface TypedField {

  String sqlExpression(QueryRewriter queryRewriter);

  Class<?> type();

  String alias();

  TypedField as(String alias);
}
