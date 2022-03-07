package me.paulbares;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.ClickHouseStatement;
import me.paulbares.store.Field;
import me.paulbares.store.Store;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;

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
    return Store.scenarioFieldName(this.name, "_");
  }

  public void setDatasource(ClickHouseDataSource dataSource) {
    this.dataSource = dataSource;

    try (ClickHouseConnection conn = this.dataSource.getConnection();
         ClickHouseStatement stmt = conn.createStatement()) {
      stmt.execute("drop table if exists " + tableName());
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
      stmt.execute("create table " + tableName() + sb + "engine=Memory");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public String tableName() {
    return ClickHouseDatastore.DB_NAME + "." + this.name;
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
    String pattern = "insert into " + tableName() + " values(" + join + ")";
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

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Field category = new Field("category", String.class);
    Field price = new Field("price", double.class);
    Field qty = new Field("quantity", int.class);

    String myStore = "myStore";
    ClickHouseStore store = new ClickHouseStore(myStore, List.of(category, price, qty));
    ClickHouseDatastore clickHouseDatastore = new ClickHouseDatastore(store);

    store.load(MAIN_SCENARIO_NAME, List.of(
            new Object[]{"drink", 2d, 10},
            new Object[]{"food", 3d, 20},
            new Object[]{"cloth", 10d, 3}
    ));

    // only HTTP and gRPC are supported at this point
    ClickHouseProtocol preferredProtocol = ClickHouseProtocol.HTTP;
    // you'll have to parse response manually if use different format
    ClickHouseFormat preferredFormat = ClickHouseFormat.RowBinaryWithNamesAndTypes;
    // connect to localhost, use default port of the preferred protocol
    ClickHouseNode server = ClickHouseNode.builder().port(preferredProtocol).build();

//    ClickHouseClient client = ClickHouseClient.newInstance(preferredProtocol);
//    ClickHouseResponse show_databases = client.connect(server)
//            .format(preferredFormat)
//            .query("show databases")
//            .execute()
//            .get();
//    System.out.println(show_databases);
//
//    ClickHouseResponse clickHouseResponse = client.connect(server)
//            .format(preferredFormat)
//            .query("show TABLES from " + ClickHouseDatastore.DB_NAME)
//            .execute()
//            .get();
//    System.out.println(clickHouseResponse);

    try (ClickHouseClient client = ClickHouseClient.newInstance(preferredProtocol);
         ClickHouseResponse response = client.connect(server)
                 .format(preferredFormat)
                 .query("select * from " + store.tableName())
//                 .params(1000)
                 .execute()
                 .get()) {
      // or resp.stream() if you prefer stream API
      for (ClickHouseRecord record : response.records()) {
        record.forEach(System.out::print);
        System.out.println();
      }

      ClickHouseResponseSummary summary = response.getSummary();
      long totalRows = summary.getTotalRowsToRead();
      System.out.println(totalRows);
    }

  }
}
