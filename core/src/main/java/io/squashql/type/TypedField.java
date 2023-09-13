package io.squashql.type;

import io.squashql.query.database.QueryRewriter;

public interface TypedField {

  String store();

  String name();

  Class<?> type();

  String sqlExpression(QueryRewriter queryRewriter, boolean withAlias);
}
