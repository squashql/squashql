package io.squashql;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardSQLTypeName;
import io.squashql.jdbc.JdbcUtil;
import io.squashql.list.Lists;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;

public final class BigQueryUtil {

  private BigQueryUtil() {
  }

  public static Class<?> bigQueryTypeToClass(Field field) {
    return _bigQueryTypeToClass(field, true);
  }

  private static Class<?> _bigQueryTypeToClass(Field field, boolean top) {
    if (top && field.getMode() == Field.Mode.REPEATED) {
      // it is an array
      return JdbcUtil.getListClassFromElementClass(_bigQueryTypeToClass(field, false));
    } else {
      LegacySQLTypeName dataType = field.getType();
      return switch (dataType.getStandardType()) {
        case BOOL -> boolean.class;
        case INT64 -> long.class;
        case FLOAT64 -> double.class;
        case STRING -> String.class;
        case BYTES -> byte.class;
        case DATE -> LocalDate.class;
        case DATETIME -> LocalDateTime.class;
        default -> Object.class;
      };
    }
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
      type = StandardSQLTypeName.INT64; // there is no INT32 in bigquery
    } else if (clazz.equals(Long.class) || clazz.equals(long.class)) {
      type = StandardSQLTypeName.INT64;
    } else if (clazz.equals(Boolean.class) || clazz.equals(boolean.class)) {
      type = StandardSQLTypeName.BOOL;
    } else if (clazz.equals(LocalDate.class)) {
      type = StandardSQLTypeName.DATE;
    } else if (clazz.equals(LocalDateTime.class)) {
      type = StandardSQLTypeName.DATETIME;
    } else if (clazz.equals(Lists.LongList.class)) {
      type = StandardSQLTypeName.ARRAY;
    } else if (clazz.equals(Lists.StringList.class)) {
      type = StandardSQLTypeName.ARRAY;
    } else {
      throw new IllegalArgumentException("Unsupported field type " + clazz);
    }
    return type;
  }

  /**
   * Creates {@link ServiceAccountCredentials} from a file.
   * See <a href="https://cloud.google.com/bigquery/docs/authentication/service-account-file">https://cloud.google.com/bigquery/docs/authentication/service-account-file</a>
   *
   * @param path path to the service account key file.
   * @return the {@link ServiceAccountCredentials}
   */
  public static ServiceAccountCredentials createCredentialsFromFile(String path) {
    try {
      InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
      if (resourceAsStream == null) {
        resourceAsStream = new FileInputStream(path);
      }
      return ServiceAccountCredentials.fromStream(resourceAsStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static ServiceAccountCredentials createCredentialsFromFileContent(String jsonKey) {
    try {
      return ServiceAccountCredentials.fromStream(new ByteArrayInputStream(jsonKey.getBytes()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
