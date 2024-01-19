package io.squashql.type;

import io.squashql.query.database.QueryRewriter;

import java.util.Objects;

public record ConstantTypedField(Object value) implements TypedField {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter) {
    return Objects.toString(this.value);
  }

  @Override
  public Class<?> type() {
    return this.value.getClass();
  }

  @Override
  public String alias() {
    throw new IllegalStateException("Incorrect path of execution");
  }

  @Override
  public TypedField as(String alias) {
    throw new IllegalStateException("Incorrect path of execution");
  }
}
