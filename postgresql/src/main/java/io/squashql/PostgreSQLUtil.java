package io.squashql;


import io.squashql.jdbc.JdbcUtil;
import io.squashql.util.Types;

import java.math.BigDecimal;

public final class PostgreSQLUtil {

  private PostgreSQLUtil() {
  }

  public static String classToPostgreType(Class<?> clazz) {
    if (clazz.equals(String.class)) {
      return "TEXT";
    } else if (clazz.equals(double.class)) {
      return "double precision";
    } else {
      return JdbcUtil.classToSqlType(clazz);
    }
  }

  public static Object getTypeValue(Object o) {
    if (o instanceof BigDecimal bd) {
      return Types.castToDouble(bd);
    } else {
      return o;
    }
  }
}
