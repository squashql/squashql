package io.squashql.type;

import io.squashql.query.database.QueryRewriter;

import java.util.function.Function;

//todo-181 should it go to table package ?
public record TableField(String store, String fieldName, Class<?> type) implements TypedField {

  @Override
  public String sqlExpression(Function<String, TableField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
    return null;
  }
}
