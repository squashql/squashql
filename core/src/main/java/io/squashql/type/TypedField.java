package io.squashql.type;

import io.squashql.query.database.QueryRewriter;

import java.util.function.Function;

public interface TypedField {

  String store();

  String fieldName();

  Class<?> type();

  String sqlExpression(Function<String, TableField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias);
}
