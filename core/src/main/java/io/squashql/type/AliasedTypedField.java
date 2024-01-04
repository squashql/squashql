package io.squashql.type;

import io.squashql.query.database.QueryRewriter;
import io.squashql.store.UnknownType;

public record AliasedTypedField(String alias) implements TypedField {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter) {
    return queryRewriter.escapeAlias(this.alias);
  }

  @Override
  public Class<?> type() {
    return UnknownType.class;
  }

  @Override
  public String name() {
    return this.alias;
  }

  @Override
  public TypedField as(String alias) {
    return new AliasedTypedField(alias); // does not make sense...
  }
}
