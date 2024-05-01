package io.squashql;


import io.squashql.jdbc.JdbcUtil;
import io.squashql.list.Lists;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public final class PostgreSQLUtil {

  private PostgreSQLUtil() {
  }

  /**
   * It is used in tests by the PostgreSQL {@link io.squashql.transaction.DataLoader} impl.
   */
  public static String classToPostgreSQLType(Class<?> clazz) {
    if (clazz.equals(String.class)) {
      return "text";
    } else if (clazz.equals(double.class)) {
      return "double precision";
    } else if (clazz.equals(Lists.LongList.class)) {
      return "integer[]";
    } else if (clazz.equals(Lists.StringList.class)) {
      return "varchar[]";
    } else if (clazz.equals(Object.class)) {
      return "json";
    } else {
      return JdbcUtil.classToSqlType(clazz);
    }
  }

  public static Class<?> getJavaClass(ResultSetMetaData metaData, int columnIndex) throws SQLException {
    String columnTypeName = metaData.getColumnTypeName(columnIndex);
    String columnClassName = metaData.getColumnClassName(columnIndex);
    if (columnTypeName.equals("numeric")) {
      return columnClassName.equals(BigDecimal.class.getName()) ? BigDecimal.class : BigInteger.class;
    } else {
      return getJavaClass(metaData.getColumnType(columnIndex), columnTypeName);
    }
  }

  public static Class<?> getJavaClass(int dataType, String columnTypeName) {
    if (columnTypeName.equals("numeric")) {
      return BigDecimal.class; // FIXME this might be wrong
    } else if (columnTypeName.equals("_int4") || columnTypeName.equals("_int8") || columnTypeName.equals("_numeric")) {
      return Lists.LongList.class;
    } else if (columnTypeName.equals("_float4") || columnTypeName.equals("_float8")) {
      return Lists.DoubleList.class;
    } else if (columnTypeName.equals("_varchar")) {
      return Lists.StringList.class;
    } else if (columnTypeName.equals("_date")) {
      return Lists.LocalDateList.class;
    } else {
      return JdbcUtil.sqlTypeToClass(dataType);
    }
  }
}
