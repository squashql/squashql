package me.paulbares.query.database;

import me.paulbares.query.Measure;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlUtils {

  static final Pattern GROUPING_PATTERN = Pattern.compile("___grouping___(.*)___");

  public static String escape(String column) {
    return "`" + column + "`";
  }

  public static String appendAlias(String sql, QueryRewriter queryRewriter, String alias, Measure measure) {
    return sql + " as " + queryRewriter.measureAlias(escape(alias), measure);
  }

  public static String extractGroupingField(String str) {
    Matcher matcher = GROUPING_PATTERN.matcher(str);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }

  // FIXME to delete
  public static void main(String[] args) {
    System.out.println(extractGroupingField(SQLTranslator.groupingAlias("toto")));
  }
}
