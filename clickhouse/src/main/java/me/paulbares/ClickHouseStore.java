package me.paulbares;

import com.clickhouse.client.ClickHouseDataType;
import me.paulbares.store.Field;
import me.paulbares.store.Store;

import java.util.ArrayList;
import java.util.List;

public class ClickHouseStore implements Store {

  protected final String name;

  protected final List<Field> fields;

  public ClickHouseStore(String name, List<Field> fields) {
    this.name = name;
    this.fields = new ArrayList<>(fields);
    this.fields.add(new Field(scenarioFieldName(), String.class));
  }

  @Override
  public String scenarioFieldName() {
    return Store.scenarioFieldName(this.name, "_"); // use a different separator because issue with dot '.'
  }

  @Override
  public String name() {
    return this.name;
  }

  @Override
  public List<Field> getFields() {
    return this.fields;
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
      default -> throw new IllegalArgumentException("Unsupported data type " + dataType);
    };
  }
}
