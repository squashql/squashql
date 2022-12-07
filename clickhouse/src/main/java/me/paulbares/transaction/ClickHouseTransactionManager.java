package me.paulbares.transaction;

import com.clickhouse.client.*;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.ClickHouseStatement;
import me.paulbares.ClickHouseDatastore;
import me.paulbares.store.Field;
import org.eclipse.collections.impl.list.immutable.ImmutableListFactoryImpl;

import java.io.FileNotFoundException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static me.paulbares.ClickHouseUtil.classToClickHouseType;

public class ClickHouseTransactionManager implements TransactionManager {

  protected final ClickHouseDataSource clickHouseDataSource;

  public ClickHouseTransactionManager(ClickHouseDataSource clickHouseDataSource) {
    this.clickHouseDataSource = clickHouseDataSource;
  }

  public void dropAndCreateInMemoryTable(String table, List<Field> fields) {
    dropAndCreateInMemoryTable(this.clickHouseDataSource, table, fields, true);
  }

  public static void dropAndCreateInMemoryTable(ClickHouseDataSource clickHouseDataSource, String table, List<Field> fields, boolean cjMode) {
    List<Field> list = cjMode ? ImmutableListFactoryImpl.INSTANCE
            .ofAll(fields)
            .newWith(new Field(SCENARIO_FIELD_NAME, String.class))
            .castToList() : fields;

    try (ClickHouseConnection conn = clickHouseDataSource.getConnection();
         ClickHouseStatement stmt = conn.createStatement()) {
      stmt.execute("drop table if exists " + table);
      StringBuilder sb = new StringBuilder();
      sb.append("(");
      int size = list.size();
      for (int i = 0; i < size; i++) {
        Field field = list.get(i);
        sb.append(field.name()).append(" Nullable(").append(classToClickHouseType(field.type())).append(')');
        if (i < size - 1) {
          sb.append(", ");
        }
      }
      sb.append(")");
      stmt.execute("create table " + table + sb + "engine=Memory");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void load(String scenario, String store, List<Object[]> tuples) {
    // Check the table contains a column scenario.
    ensureScenarioColumnIsPresent(store);
    String join = String.join(",", IntStream.range(0, tuples.get(0).length + 1).mapToObj(i -> "?").toList());
    String pattern = "insert into " + store + " values(" + join + ")";
    try (ClickHouseConnection conn = this.clickHouseDataSource.getConnection();
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

  private void ensureScenarioColumnIsPresent(String store) {
    List<Field> fields = ClickHouseDatastore.getFields(this.clickHouseDataSource, store);
    boolean found = fields.stream().anyMatch(f -> f.name().equals(SCENARIO_FIELD_NAME));
    if (!found) {
      throw new RuntimeException(String.format("%s field not found", SCENARIO_FIELD_NAME));
    }
  }

  @Override
  public void loadCsv(String scenario, String store, String path, String delimiter, boolean header) {
    ClickHouseNode clickHouseNode = ClickHouseNode.of(this.clickHouseDataSource.getHost(),
            ClickHouseProtocol.HTTP,
            this.clickHouseDataSource.getPort(),
            null);

    try {
      CompletableFuture<ClickHouseResponseSummary> load = ClickHouseClient.load(
              clickHouseNode,
              store,
              header ? ClickHouseFormat.CSVWithNames : ClickHouseFormat.CSV,
              ClickHouseCompression.LZ4,
              path);
      load.get();
    } catch (FileNotFoundException | InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
