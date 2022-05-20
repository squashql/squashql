package me.paulbares;

import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.ClickHouseStatement;
import com.google.common.base.Suppliers;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.store.Store;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

public class ClickHouseDatastore implements Datastore {

  public final Supplier<Map<String, Store>> stores;

  public final ClickHouseDataSource dataSource;

  public ClickHouseDatastore(String jdbc, String databaseName) {
    this.dataSource = newDataSource(jdbc, null);

    if (databaseName != null) {
      try (ClickHouseConnection conn = this.dataSource.getConnection();
           ClickHouseStatement stmt = conn.createStatement()) {
        stmt.execute("CREATE DATABASE IF NOT EXISTS " + databaseName);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    this.stores = Suppliers.memoize(() -> {
      Map<String, Store> r = new HashMap<>();
      getTableNames(this.dataSource).forEach(table -> r.put(table, new Store(table,
              getFields(this.dataSource, table))));
      return r;
    });
  }

  public ClickHouseDataSource getDataSource() {
    return this.dataSource;
  }

  @Override
  public Map<String, Store> storesByName() {
    return this.stores.get();
  }

  private static ClickHouseDataSource newDataSource(String jdbcUrl, Properties properties) {
    try {
      return new ClickHouseDataSource(jdbcUrl, properties);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static Collection<String> getTableNames(ClickHouseDataSource dataSource) {
    try {
      DatabaseMetaData metaData = dataSource.getConnection().getMetaData();
      ResultSet tables = metaData.getTables(null, "default", null, null);

      Set<String> tableNames = new HashSet<>();
      while (tables.next()) {
        tableNames.add((String) tables.getObject("TABLE_NAME"));
      }

      return tableNames;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<Field> getFields(ClickHouseDataSource dataSource, String table) {
    try {
      DatabaseMetaData metaData = dataSource.getConnection().getMetaData();
      ResultSet columns = metaData.getColumns(null, "default", table, null);

      List<Field> fields = new ArrayList<>();
      while (columns.next()) {
        String columnName = (String) columns.getObject("COLUMN_NAME");
        String typeName = (String) columns.getObject("TYPE_NAME");
        ClickHouseDataType dataType = ClickHouseDataType.of(typeName);
        fields.add(new Field(columnName, ClickHouseUtil.clickHouseTypeToClass(dataType)));
      }

      return fields;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
