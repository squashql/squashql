package io.squashql.type;

import io.squashql.query.database.QueryRewriter;

import java.util.function.Function;

public interface TypedField {

  String store();

  String name();

  Class<?> type();

  String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias);
}
