package me.paulbares;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.LegacySQLTypeName;

import java.io.IOException;
import java.io.InputStream;

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

  public static ServiceAccountCredentials createCredentials(String path) {
    try {
      InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
      return ServiceAccountCredentials.fromStream(resourceAsStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
