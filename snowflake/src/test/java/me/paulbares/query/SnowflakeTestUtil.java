package me.paulbares.query;

public class SnowflakeTestUtil {

  public static final String jdbcUrl = "jdbc:snowflake://<account_identifier>.snowflakecomputing.com";
  public static final String username = "";
  public static final String password = "";
  public static final String warehouse = "";
  public static final String database = "";
  public static final String schema = "";

  /**
   * See {@link me.paulbares.SnowflakeUtil}.
   */
  public static Object translate(Object o) {
    if (o == null) {
      return null;
    }

    if (o.getClass().equals(int.class) || o.getClass().equals(Integer.class)) {
      return ((Number) o).longValue();
    } else {
      return o;
    }
  }
}
