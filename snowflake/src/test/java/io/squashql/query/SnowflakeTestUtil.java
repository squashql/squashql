package io.squashql.query;

import java.util.Properties;

public class SnowflakeTestUtil {

  public static final String jdbcUrl = "jdbc:snowflake://<account_identifier>.snowflakecomputing.com";
  public static final String schema = "";
  public static final String database = "";

  public static final Properties properties = new Properties();

  static {
    properties.put("user", "");
    properties.put("password", "");
    properties.put("warehouse", "");
    properties.put("role", "");
  }

  /**
   * See {@link io.squashql.SnowflakeUtil}.
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
