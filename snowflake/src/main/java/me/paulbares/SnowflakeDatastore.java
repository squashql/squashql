package me.paulbares;

import com.google.common.base.Suppliers;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.store.Store;

import java.util.*;
import java.util.function.Supplier;

public class SnowflakeDatastore implements Datastore {

  private final Statement snowflake;
  public final Supplier<Map<String, Store>> stores;

  public SnowflakeDatastore() throws SQLException {
    // build connection properties
    Properties properties = new Properties();
    properties.put("user", ""); // replace "" with your user name
    properties.put("password", ""); // replace "" with your password
    properties.put("warehouse", ""); // replace "" with target warehouse name
    properties.put("db", ""); // replace "" with target database name
    properties.put("schema", ""); // replace "" with target schema name
    // properties.put("tracing", "all"); // optional tracing property

    // Replace <account_identifier> with your account identifier. See
    // https://docs.snowflake.com/en/user-guide/admin-account-identifier.html
    // for details.
    String connectStr = "jdbc:snowflake://<account_identifier>.snowflakecomputing.com";
    Connection connection = DriverManager.getConnection(connectStr, properties);
    this.snowflake = connection.createStatement();

    DatabaseMetaData metadata = connection.getMetaData();
    this.stores = Suppliers.memoize(() -> getStores(metadata));
  }

  public Statement getSnowflake() {
    return this.snowflake;
  }

  @Override
  public Map<String, Store> storesByName() {
    return this.stores.get();
  }

  public static Map<String, Store> getStores(DatabaseMetaData metadata) {
    try {
      Map<String, Store> stores = new HashMap<>();
      ResultSet resultSet = metadata.getColumns("OPTIPRIX", "PUBLIC", "%", null);
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
