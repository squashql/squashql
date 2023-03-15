package io.squashql.query.database;

import io.squashql.store.Field;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlUtils {

  static final Pattern GROUPING_PATTERN = Pattern.compile("___grouping___(.*)___");

  public static String backtickEscape(String column) {
    return "`" + column + "`";
  }

  public static String doubleQuoteEscape(String column) {
    return "\"" + column + "\"";
  }

  public static String appendAlias(String sql, QueryRewriter queryRewriter, String alias) {
    return sql + " as " + queryRewriter.measureAlias(alias);
  }

  public static String getFieldFullName(String store, String name) {
    return store == null ? name : store + '.' + name;
  }

  public static String getFieldFullName(Field field) {
    return field.store() == null ? field.name() : field.store() + '.' + field.name();
  }

  /**
   * See {@link QueryRewriter#groupingAlias(String)}.
   */
  public static String extractFieldFromGroupingAlias(String str) {
    Matcher matcher = GROUPING_PATTERN.matcher(str);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }
}
