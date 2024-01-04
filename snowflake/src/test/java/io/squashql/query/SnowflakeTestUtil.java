package io.squashql.query;

import java.util.Properties;

public class SnowflakeTestUtil {

  public static final String jdbcUrl = System.getenv().getOrDefault("SNOWFLAKE_JDBC_URL", System.getProperty("snowflake.test.jdbc.url"));
  public static final String schema = "PUBLIC";
  public static final String database = "TEST";

  public static final Properties properties = new Properties();

  static {
    properties.put("user", System.getenv().getOrDefault("SNOWFLAKE_USER", System.getProperty("snowflake.test.user", "")));
    properties.put("password", System.getenv().getOrDefault("SNOWFLAKE_PASSWORD", System.getProperty("snowflake.test.password", "")));
    properties.put("warehouse", "COMPUTE_WH");
    properties.put("role", "ACCOUNTADMIN");
  }

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
