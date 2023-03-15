package io.squashql;

import com.google.common.base.Suppliers;
import io.squashql.store.Datastore;
import io.squashql.store.FieldWithStore;
import io.squashql.store.Store;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

public class SnowflakeDatastore implements Datastore {

  private final String jdbcUrl;
  private final Properties connectionProperties;
  public final Supplier<Map<String, Store>> stores;

  /**
   * Constructor.
   * @param jdbcUrl a database url of the form jdbc:subprotocol:subname
   * @param info a list of arbitrary string tag/value pairs as connection arguments; normally at least a "user" and
   *             "password" property should be included
   */
  public SnowflakeDatastore(String jdbcUrl,
                            String database,
                            String schema,
                            Properties info) {
    this.jdbcUrl = jdbcUrl;
    // Build connection properties
    Properties properties = new Properties();
    properties.putAll(info);
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
          value.fields().add(new FieldWithStore(tableName, columnName, SnowflakeUtil.sqlTypeToClass(dataType)));
          return value;
        });
      }
      return stores;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
