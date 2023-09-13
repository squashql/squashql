package io.squashql.type;

import io.squashql.query.database.QueryRewriter;
import io.squashql.store.UnknownType;

public record FunctionField(String store, String name) implements TypedField {

  @Override
  public Class<?> type() {
    return UnknownType.class;
  }

  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    return queryRewriter.selectDate(this);
//    for (Pattern p : DATE_PATTERNS) {
//      Matcher matcher = p.matcher(this.name);
//      if (matcher.find()) {
//        return matcher.group(1) + "(" + matcher.group(2) + ")";
//      }
//    }
//    throw new UnsupportedOperationException("Unsupported function: " + this.name);
  }
}
