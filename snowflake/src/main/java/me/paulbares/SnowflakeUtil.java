package me.paulbares;

import java.math.BigDecimal;
import java.sql.Types;

public final class SnowflakeUtil {

  private SnowflakeUtil() {
  }

  public static Class<?> sqlTypeToClass(int dataType) {
    return switch (dataType) {
      case Types.CHAR, Types.NVARCHAR, Types.VARCHAR, Types.LONGVARCHAR -> String.class;
      case Types.NUMERIC, Types.DECIMAL -> java.math.BigDecimal.class;
      case Types.BOOLEAN, Types.BIT -> boolean.class;
      case Types.TINYINT -> byte.class;
      case Types.SMALLINT -> short.class;
      case Types.INTEGER -> int.class;
      case Types.BIGINT -> long.class;
      case Types.REAL, Types.FLOAT -> float.class;
      case Types.DOUBLE -> double.class;
      case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> byte[].class;
      case Types.DATE -> java.sql.Date.class;
      case Types.TIME -> java.sql.Time.class;
      case Types.TIMESTAMP -> java.sql.Timestamp.class;
      default -> Object.class;
    };
  }

  public static String classToSqlType(Class<?> clazz) {
    if (clazz.equals(String.class)) {
      return "STRING";
    }
    if (clazz.equals(BigDecimal.class)) {
      return "NUMBER";
    }
    if (clazz.equals(Byte.class) || clazz.equals(byte.class)) {
      return "TINYINT";
    }
    if (clazz.equals(Short.class) || clazz.equals(short.class)) {
      return "SMALLINT";
    }
    if (clazz.equals(Integer.class) || clazz.equals(int.class)) {
      return "INTEGER";
    }
    if (clazz.equals(Long.class) || clazz.equals(long.class)) {
      return "BIGINT";
    }
    if (clazz.equals(Float.class) || clazz.equals(float.class)) {
      return "FLOAT";
    }
    if (clazz.equals(Double.class) || clazz.equals(double.class)) {
      return "DOUBLE";
    }
    if (clazz.equals(Boolean.class) || clazz.equals(boolean.class)) {
      return "BOOLEAN";
    }
    if (clazz.equals(java.sql.Date.class)) {
      return "DATE";
    }
    if (clazz.equals(java.sql.Time.class)) {
      return "TIME";
    }
    if (clazz.equals(java.sql.Timestamp.class)) {
      return "TIMESTAMP";
    }
    throw new IllegalArgumentException("Unsupported field type " + clazz);
  }

}
