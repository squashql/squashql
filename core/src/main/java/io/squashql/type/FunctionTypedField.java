package io.squashql.type;

import io.squashql.query.database.QueryRewriter;
import io.squashql.store.UnknownType;

import static io.squashql.query.date.DateFunctions.SUPPORTED_DATE_FUNCTIONS;

public record FunctionTypedField(TableTypedField field, String function, String alias) implements TypedField {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter) {
    return queryRewriter.functionExpression(this);
  }

  @Override
  public Class<?> type() {
    if (SUPPORTED_DATE_FUNCTIONS.contains(this.function)) {
      return int.class;
    } else {
      return UnknownType.class;
    }
  }

  @Override
  public String name() {
    throw new IllegalStateException("Incorrect path of execution");
  }
}
