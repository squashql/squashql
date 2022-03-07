package me.paulbares;

import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.ClickHouseStatement;
import me.paulbares.store.Field;
import me.paulbares.store.Store;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class ClickHouseStore implements Store {

  protected final String name;

  protected final List<Field> fields;

  protected ClickHouseDataSource dataSource;

  public ClickHouseStore(String name, List<Field> fields) {
    this.name = name;
    this.fields = new ArrayList<>(fields);
    this.fields.add(new Field(scenarioFieldName(), String.class));
  }

  @Override
  public String scenarioFieldName() {
    return Store.scenarioFieldName(this.name, "_"); // use a different separator because issue with dot '.'
  }

  public void setDatasource(ClickHouseDataSource dataSource) {
    this.dataSource = dataSource;

    try (ClickHouseConnection conn = this.dataSource.getConnection();
         ClickHouseStatement stmt = conn.createStatement()) {
      stmt.execute("drop table if exists " + this.name);
      StringBuilder sb = new StringBuilder();
      sb.append("(");
      for (int i = 0; i < this.fields.size(); i++) {
        Field field = this.fields.get(i);
        sb.append(field.name()).append(' ').append(classToClickHouseType(field.type()));
        if (i < this.fields.size() - 1) {
          sb.append(", ");
        }
      }
      sb.append(")");
      stmt.execute("create table " + this.name + sb + "engine=Memory");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String name() {
    return this.name;
  }

  @Override
  public List<Field> getFields() {
    return this.fields;
  }

  @Override
  public void load(String scenario, List<Object[]> tuples) {
    String join = String.join(",", IntStream.range(0, this.fields.size()).mapToObj(i -> "?").toList());
    String pattern = "insert into " + this.name + " values(" + join + ")";
    try (ClickHouseConnection conn = this.dataSource.getConnection();
         PreparedStatement stmt = conn.prepareStatement(pattern)) {

      for (Object[] tuple : tuples) {
        for (int i = 0; i < tuple.length; i++) {
          stmt.setObject(i + 1, tuple[i]);
        }
        stmt.setObject(tuple.length + 1, scenario);
        stmt.addBatch();
      }
      stmt.executeBatch();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void loadCsv(String scenario, String path, String delimiter, boolean header) {
    throw new RuntimeException("not implemented");
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
      case Int64 -> long.class;
      case Float32 -> float.class;
      case Float64 -> double.class;
      case String -> String.class;
      default -> throw new IllegalArgumentException("Unsupported data type " + dataType);
    };
  }
}
