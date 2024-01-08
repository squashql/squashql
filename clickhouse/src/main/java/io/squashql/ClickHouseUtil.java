package io.squashql;


import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import io.squashql.list.Lists;

import java.time.LocalDate;
import java.util.List;

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

  public static Class<?> clickHouseTypeToClass(ClickHouseColumn column) {
    ClickHouseDataType dataType = column.getDataType();
    return switch (dataType) {
      case Array -> {
        ClickHouseColumn baseColumn = column.getArrayBaseColumn();
        Class<?> elementClass = clickHouseTypeToClass(baseColumn);
        if (elementClass.equals(double.class) || elementClass.equals(float.class)) {
          yield Lists.DoubleList.class;
        } else if (elementClass.equals(long.class) || elementClass.equals(int.class)) {
          yield Lists.LongList.class;
        } else {
          yield List.class; // we convert Array to List
        }
      }
      default -> dataType.getPrimitiveClass();
    };
  }
}
