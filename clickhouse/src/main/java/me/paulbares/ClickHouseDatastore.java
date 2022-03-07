package me.paulbares;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.ClickHouseStatement;
import me.paulbares.store.Datastore;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ClickHouseDatastore implements Datastore {

  public static final String JDBC_URL = "jdbc:clickhouse://localhost:" + ClickHouseProtocol.HTTP.getDefaultPort();

  public final Map<String, ClickHouseStore> stores = new HashMap<>();

  public final ClickHouseDataSource dataSource;

  public ClickHouseDatastore(ClickHouseStore... stores) {
    this(null, stores);
  }

  public ClickHouseDatastore(String databaseName, ClickHouseStore... stores) {
    this(JDBC_URL, databaseName, stores);
  }

  public ClickHouseDatastore(String jdbc, String databaseName, ClickHouseStore... stores) {
    this.dataSource = newDataSource(jdbc, null);

    if (databaseName != null) {
      try (ClickHouseConnection conn = this.dataSource.getConnection();
           ClickHouseStatement stmt = conn.createStatement()) {
        stmt.execute("CREATE DATABASE IF NOT EXISTS " + databaseName);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    for (ClickHouseStore store : stores) {
      store.setDatasource(this.dataSource);
      this.stores.put(store.name(), store);
    }
  }

  public static ClickHouseDataSource newDataSource(String jdbcUrl, Properties properties) {
    try {
      return new ClickHouseDataSource(jdbcUrl, properties);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<ClickHouseStore> stores() {
    return new ArrayList<>(this.stores.values());
  }

  @Override
  public void loadCsv(String scenario, String store, String path, String delimiter, boolean header) {
    this.stores.get(store).loadCsv(scenario, path, delimiter, header);
  }

  @Override
  public void load(String scenario, String store, List<Object[]> tuples) {
    this.stores.get(store).load(scenario, tuples);
  }
}
