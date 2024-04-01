package io.squashql;

import com.google.common.base.Suppliers;
import io.squashql.jdbc.JdbcDatastore;
import io.squashql.jdbc.JdbcUtil;
import io.squashql.store.Store;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

public class SnowflakeDatastore implements JdbcDatastore {

  private final String jdbcUrl;
  private final Properties connectionProperties;
  public final Supplier<Map<String, Store>> stores;

  /**
   * Constructor.
   *
   * @param jdbcUrl a database url of the form jdbc:subprotocol:subname
   * @param info    a list of arbitrary string tag/value pairs as connection arguments; normally at least a "user" and
   *                "password" property should be included
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
    this.stores = Suppliers.memoize(() -> JdbcUtil.getStores(database, schema, getConnection(), (dataType, __) -> JdbcUtil.sqlTypeToClass(dataType)));
  }

  @Override
  public Connection getConnection() {
    try {
      return DriverManager.getConnection(this.jdbcUrl, this.connectionProperties);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, Store> storeByName() {
    return this.stores.get();
  }
}
