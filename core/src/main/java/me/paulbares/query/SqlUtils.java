package me.paulbares.query;

public class SqlUtils {
  
  public static String escape(String column) {
    return "`" + column + "`";
  }
}
