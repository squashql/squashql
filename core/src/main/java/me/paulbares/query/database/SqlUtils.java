package me.paulbares.query.database;

import me.paulbares.query.Measure;

public class SqlUtils {

  public static String escape(String column) {
    return "`" + column + "`";
  }

  public static String appendAlias(String sql, QueryRewriter queryRewriter, String alias, Measure measure) {
    return sql + " as " + queryRewriter.measureAlias(escape(alias), measure);
  }
}
