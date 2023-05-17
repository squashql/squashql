package io.squashql;


import com.clickhouse.data.ClickHouseDataType;

import java.time.LocalDate;

public final class ClickHouseUtil {

  private ClickHouseUtil() {
  }

  public static String classToClickHouseType(Class<?> clazz) {
    String type;
    if (clazz.equals(String.class)) {
      type = ClickHouseDataType.String.name();
    } else if (clazz.equals(Double.class) || clazz.equals(double.class)) {
      type = ClickHouseDataType.Float64.name();
    } else if (clazz.equals(Float.class) || clazz.equals(float.class)) {
      type = ClickHouseDataType.Float32.name();
    } else if (clazz.equals(Integer.class) || clazz.equals(int.class)) {
      type = ClickHouseDataType.Int32.name();
    } else if (clazz.equals(Long.class) || clazz.equals(long.class)) {
      type = ClickHouseDataType.Int64.name();
    } else if (clazz.equals(Boolean.class) || clazz.equals(boolean.class)) {
      type = ClickHouseDataType.Bool.name();
    } else if (clazz.equals(LocalDate.class)) {
      type = ClickHouseDataType.Date.name();
    } else {
      throw new IllegalArgumentException("Unsupported field type " + clazz);
    }
    return type;
  }

  public static Class<?> clickHouseTypeToClass(ClickHouseDataType dataType) {
    return dataType.getObjectClass();
  }
}
