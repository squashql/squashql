package me.paulbares;

import com.clickhouse.client.ClickHouseDataType;
import me.paulbares.store.Datastore;

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
    } else if (clazz.equals(LocalDate.class)) {
      type = ClickHouseDataType.Date.name();
    } else {
      throw new IllegalArgumentException("Unsupported field type " + clazz);
    }
    return type;
  }

  public static Class<?> clickHouseTypeToClass(ClickHouseDataType dataType) {
    return switch (dataType) {
      case Int32 -> int.class;
      case Int64, UInt64 -> long.class;
      case Float32 -> float.class;
      case Float64 -> double.class;
      case String -> String.class;
      case Date -> LocalDate.class;
      default -> throw new IllegalArgumentException("Unsupported data type " + dataType);
    };
  }

  public static String getScenarioName(String storeName) {
    return storeName + "_" + Datastore.SCENARIO_FIELD_NAME;
  }
}
