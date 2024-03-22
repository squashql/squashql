package io.squashql;

import com.google.common.base.Suppliers;
import io.squashql.jdbc.JdbcDatastore;
import io.squashql.jdbc.JdbcUtil;
import io.squashql.store.Store;
import lombok.extern.slf4j.Slf4j;
import org.duckdb.DuckDBConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Supplier;

@Slf4j
public class DuckDBDatastore implements JdbcDatastore {

  private final DuckDBConnection connection;
  public final Supplier<Map<String, Store>> stores;

  public DuckDBDatastore() {
    this(true);
  }

  public DuckDBDatastore(boolean cacheMetadata) {
    try {
      Class.forName("org.duckdb.DuckDBDriver");
      // Create an in-memory db.
      this.connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
      this.stores = cacheMetadata ? Suppliers.memoize(this::fetchStoresByName) : this::fetchStoresByName;
    } catch (Exception e) {
      log.error("Connection error", e);
      throw new RuntimeException(e);
    }
  }

  public Map<String, Store> fetchStoresByName() {
    try {
      String schema = this.connection.getSchema();
      String catalog = this.connection.getCatalog();
      return JdbcUtil.getStores(catalog, schema, getConnection(), DuckDBUtil::sqlTypeToClass_);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Connection getConnection() {
    try {
      // Duplicate the connection to increase the count in DuckDBDatabase. Once it reaches 0, the db is deleted.
      return this.connection.duplicate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, Store> storesByName() {
    return this.stores.get();
  }
}
