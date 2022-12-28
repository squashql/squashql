package me.paulbares.query.database;

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

  /**
   * See {@link SQLTranslator#groupingAlias(String, QueryRewriter)}.
   */
  public static String extractFieldFromGroupingAlias(String str) {
    Matcher matcher = GROUPING_PATTERN.matcher(str);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }

}
