package io.squashql.type;

import io.squashql.query.database.QueryRewriter;
import io.squashql.store.UnknownType;

import java.util.function.Function;

public record FunctionField(String store, String fieldName) implements TypedField {

  @Override
  public Class<?> type() {
    return UnknownType.class;
  }

  public String sqlExpression(Function<String, TableField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
    return null;
  }
}
