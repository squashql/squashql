package io.squashql.type;

import io.squashql.query.database.QueryRewriter;

import java.util.function.Function;

//todo-181 should it go to table package ?
public record TableField(String store, String name, Class<?> type) implements TypedField {

  @Override
  public String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
    return queryRewriter.select(this);
  }
}
