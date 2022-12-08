package me.paulbares;

import java.sql.Types;

public final class SnowflakeUtil {

  private SnowflakeUtil() {
  }

  public static Class<?> sqlTypeToClass(int dataType) {
    return switch (dataType) {
      case Types.CHAR, Types.NVARCHAR, Types.VARCHAR, Types.LONGVARCHAR -> String.class;
      case Types.NUMERIC, Types.DECIMAL -> java.math.BigDecimal.class;
      case Types.BIT -> Boolean.class;
      case Types.TINYINT -> Byte.class;
      case Types.SMALLINT -> Short.class;
      case Types.INTEGER -> Integer.class;
      case Types.BIGINT -> Long.class;
      case Types.REAL, Types.FLOAT -> Float.class;
      case Types.DOUBLE -> Double.class;
      case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> Byte[].class;
      case Types.DATE -> java.sql.Date.class;
      case Types.TIME -> java.sql.Time.class;
      case Types.TIMESTAMP -> java.sql.Timestamp.class;
      default -> Object.class;
    };
  }

//  public static ServiceAccountCredentials createCredentials(String path) {
//    try {
//      InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
//      if (resourceAsStream == null) {
//        resourceAsStream = new FileInputStream(path);
//      }
//      return ServiceAccountCredentials.fromStream(resourceAsStream);
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }
//  }
}
