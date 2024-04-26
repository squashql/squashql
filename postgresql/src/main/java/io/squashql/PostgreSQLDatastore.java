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

public class PostgreSQLDatastore implements JdbcDatastore {

  private final String jdbcUrl;
  private final Properties connectionProperties;
  public final Supplier<Map<String, Store>> stores;

  public PostgreSQLDatastore(String jdbcUrl, Properties properties) {
    this.jdbcUrl = jdbcUrl;
    this.connectionProperties = properties;
    String schema = properties.getProperty("currentSchema", "public");
    this.stores = Suppliers.memoize(() -> JdbcUtil.getStores(properties.getProperty("database"), schema, getConnection(), PostgreSQLUtil::getJavaClass));
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
