package io.squashql.type;

import io.squashql.query.CountMeasure;
import io.squashql.query.database.QueryRewriter;

public record TableTypedField(String store, String name, Class<?> type) implements TypedField {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter) {
    if (CountMeasure.FIELD_NAME.equals(this.name)) {
      return CountMeasure.FIELD_NAME;
    } else {
      return queryRewriter.getFieldFullName(this);
    }
  }
}
