package me.paulbares.transaction;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import me.paulbares.ClickHouseDatastore;
import me.paulbares.store.Field;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static me.paulbares.transaction.TransactionManager.scenarioStoreName;

public class ClickHouseDeltaTransactionManager extends ClickHouseTransactionManager {

  private Set<String> storeAlreadyCreated = new HashSet<>();

  public ClickHouseDeltaTransactionManager(ClickHouseDataSource clickHouseDataSource) {
    super(clickHouseDataSource);
  }

  @Override
  public void dropAndCreateInMemoryTable(String table, List<Field> fields) {
    ClickHouseTransactionManager.dropAndCreateInMemoryTable(this.clickHouseDataSource, table, fields, false);
    this.storeAlreadyCreated.add(table);
  }

  @Override
  public void load(String scenario, String store, List<Object[]> tuples) {
    // Check the table contains a column scenario.
    ensureScenarioTableIsPresent(store, scenario);
    String join = String.join(",", IntStream.range(0, tuples.get(0).length).mapToObj(i -> "?").toList());
    String pattern = "insert into " + scenarioStoreName(store, scenario) + " values(" + join + ")";
    try (ClickHouseConnection conn = this.clickHouseDataSource.getConnection();
         PreparedStatement stmt = conn.prepareStatement(pattern)) {

      for (Object[] tuple : tuples) {
        for (int i = 0; i < tuple.length; i++) {
          stmt.setObject(i + 1, tuple[i]);
        }
        stmt.addBatch();
      }
      stmt.executeBatch();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void ensureScenarioTableIsPresent(String store, String scenario) {
    String storeName = scenarioStoreName(store, scenario);
    Collection<String> tableNames = ClickHouseDatastore.getTableNames(this.clickHouseDataSource);
    boolean found = tableNames.stream().anyMatch(f -> f.equals(storeName));
    if (this.storeAlreadyCreated.add(storeName) || !found) {
      // Create if not found
      dropAndCreateInMemoryTable(storeName, ClickHouseDatastore.getFields(this.clickHouseDataSource, store));
    }
  }

  @Override
  public void loadCsv(String scenario, String store, String path, String delimiter, boolean header) {
    throw new RuntimeException("not implemented yet");
  }
}
