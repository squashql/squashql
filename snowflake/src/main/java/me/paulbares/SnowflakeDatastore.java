package me.paulbares;

import com.google.common.base.Suppliers;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.store.Store;

import java.util.*;
import java.util.function.Supplier;

public class SnowflakeDatastore implements Datastore {

  private final String jdbcUrl;
  private final Properties connectionProperties;
  public final Supplier<Map<String, Store>> stores;

  public SnowflakeDatastore(String jdbcUrl, String username, String password, String warehouse, String database,
          String schema) {
    this.jdbcUrl = jdbcUrl;
    // build connection properties
    Properties properties = new Properties();
    properties.put("user", username);
    properties.put("password", password);
    properties.put("warehouse", warehouse);
    properties.put("db", database);
    properties.put("schema", schema);
    this.connectionProperties = properties;

    this.stores = Suppliers.memoize(() -> getStores(database, schema));
  }

  public Connection getConnection() throws SQLException {
    return DriverManager.getConnection(this.jdbcUrl, this.connectionProperties);
  }

  @Override
  public Map<String, Store> storesByName() {
    return this.stores.get();
  }

  Map<String, Store> getStores(String database, String schema) {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metadata = connection.getMetaData();
      Map<String, Store> stores = new HashMap<>();
      ResultSet resultSet = metadata.getColumns(database, schema, "%", null);
      while (resultSet.next()) {
        String tableName = resultSet.getString("TABLE_NAME");
        String columnName = resultSet.getString("COLUMN_NAME");
        int dataType = resultSet.getInt("DATA_TYPE");
        stores.compute(tableName, (key, value) -> {
          if (value == null) {
            value = new Store(key, new ArrayList<>());
          }
          value.fields().add(new Field(columnName, SnowflakeUtil.sqlTypeToClass(dataType)));
          return value;
        });
      }
      return stores;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

}
