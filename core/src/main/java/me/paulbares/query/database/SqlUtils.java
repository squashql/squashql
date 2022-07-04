package me.paulbares.query.database;

public class SqlUtils {

  public static String escape(String column) {
    return "`" + column + "`";
  }
}
