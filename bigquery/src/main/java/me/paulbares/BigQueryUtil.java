package me.paulbares;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardSQLTypeName;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.LocalDate;

public final class BigQueryUtil {

  private BigQueryUtil() {
  }

  public static Class<?> bigQueryTypeToClass(LegacySQLTypeName dataType) {
    return switch (dataType.getStandardType()) {
      case BOOL -> boolean.class;
      case INT64 -> long.class;
      case FLOAT64 -> double.class;
      case STRING -> String.class;
      case BYTES -> byte.class;
      default -> throw new IllegalArgumentException("Unsupported data type " + dataType);
    };
  }

  public static StandardSQLTypeName classToBigQueryType(Class<?> clazz) {
    StandardSQLTypeName type;
    if (clazz.equals(String.class)) {
      type = StandardSQLTypeName.STRING;
    } else if (clazz.equals(Double.class) || clazz.equals(double.class)) {
      type = StandardSQLTypeName.FLOAT64;
    } else if (clazz.equals(Float.class) || clazz.equals(float.class)) {
      type = StandardSQLTypeName.FLOAT64;
    } else if (clazz.equals(Integer.class) || clazz.equals(int.class)) {
      type = StandardSQLTypeName.INT64;
    } else if (clazz.equals(Long.class) || clazz.equals(long.class)) {
      type = StandardSQLTypeName.INT64;
    } else {
      throw new IllegalArgumentException("Unsupported field type " + clazz);
    }
    return type;
  }

  public static ServiceAccountCredentials createCredentials(String path) {
    try {
      return ServiceAccountCredentials.fromStream(new FileInputStream(path));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
