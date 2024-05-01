package io.squashql.type;

import io.squashql.query.database.QueryRewriter;
import io.squashql.store.UnknownType;

import static io.squashql.query.database.SqlFunctions.SUPPORTED_DATE_FUNCTIONS;
import static io.squashql.query.database.SqlFunctions.SUPPORTED_STRING_FUNCTIONS;

public record FunctionTypedField(TypedField field, String function, String alias) implements TypedField {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter) {
    return queryRewriter.functionExpression(this);
  }

  @Override
  public Class<?> type() {
    if (SUPPORTED_DATE_FUNCTIONS.contains(this.function)) {
      return int.class;
    } else if (SUPPORTED_STRING_FUNCTIONS.contains(this.function)) {
      return String.class;
    } else {
      return UnknownType.class;
    }
  }

  @Override
  public String name() {
    throw new IllegalStateException("Incorrect path of execution");
  }

  @Override
  public TypedField as(String alias) {
    return new FunctionTypedField(this.field, this.function, alias);
  }
}
