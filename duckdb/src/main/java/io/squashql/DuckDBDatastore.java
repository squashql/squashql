package io.squashql;

import io.squashql.store.Datastore;
import io.squashql.store.Field;
import io.squashql.store.Store;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DuckDBDatastore implements Datastore {

  public DuckDBDatastore() {
    try {
      Class.forName("org.duckdb.DuckDBDriver");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Connection getConnection() {
    try {
      return DriverManager.getConnection("jdbc:duckdb:data.db");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, Store> storesByName() {
    // FIXME
    try (Connection connection = getConnection()) {
      DatabaseMetaData metadata = connection.getMetaData();
      String schema = connection.getSchema();
      String catalog = connection.getCatalog();
      Map<String, Store> stores = new HashMap<>();
      ResultSet resultSet = metadata.getColumns(catalog, schema, "%", null);
      while (resultSet.next()) {
        String tableName = resultSet.getString("TABLE_NAME");
        String columnName = resultSet.getString("COLUMN_NAME");
        int dataType = resultSet.getInt("DATA_TYPE");
        stores.compute(tableName, (key, value) -> {
          if (value == null) {
            value = new Store(key, new ArrayList<>());
          }
          value.fields().add(new Field(tableName, columnName, SnowflakeUtil.sqlTypeToClass(dataType)));
          return value;
        });
      }
      return stores;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
