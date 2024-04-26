package io.squashql;


import io.squashql.jdbc.JdbcUtil;
import io.squashql.list.Lists;
import io.squashql.util.Types;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class PostgreSQLUtil {

  private PostgreSQLUtil() {
  }

  public static String classToPostgreType(Class<?> clazz) {
    if (clazz.equals(String.class)) {
      return "TEXT";
    } else if (clazz.equals(double.class)) {
      return "double precision";
    } else if (clazz.equals(Lists.LongList.class)) {
      return "integer[]";
    } else if (clazz.equals(Lists.StringList.class)) {
      return "varchar[]";
    } else {
      return JdbcUtil.classToSqlType(clazz);
    }
  }

  public static Class<?> sqlTypeToJavaClass(int dataType, String columnTypeName) {
    if (columnTypeName.equals("numeric")) {
      return BigInteger.class;
    } else if (columnTypeName.equals("_int4") || columnTypeName.equals("_int8") || columnTypeName.equals("_numeric")) {
      return Lists.LongList.class;
    } else if (columnTypeName.equals("_float4") || columnTypeName.equals("_float8")) {
      return Lists.DoubleList.class;
    } else if (columnTypeName.equals("_varchar")) {
      return Lists.StringList.class;
    } else {
      return JdbcUtil.sqlTypeToClass(dataType);
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
