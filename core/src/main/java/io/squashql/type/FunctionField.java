package io.squashql.type;

import io.squashql.query.database.QueryRewriter;
import io.squashql.store.UnknownType;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.squashql.query.date.DateFunctions.DATE_PATTERNS;

public record FunctionField(String store, String name) implements TypedField {

  @Override
  public Class<?> type() {
    return UnknownType.class;
  }

  public String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
    for (Pattern p : DATE_PATTERNS) {
      Matcher matcher = p.matcher(this.name);
      if (matcher.find()) {
        return matcher.group(1) + "(" + matcher.group(2) + ")";
      }
    }
    throw new UnsupportedOperationException("Unsupported function: " + this.name);
  }
}
