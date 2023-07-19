package io.squashql;

import com.clickhouse.client.ClickHouseNodes;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.internal.ClickHouseJdbcUrlParser;
import com.google.common.base.Suppliers;
import io.squashql.store.Datastore;
import io.squashql.store.TypedField;
import io.squashql.store.Store;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Supplier;

public class ClickHouseDatastore implements Datastore {

  public final Supplier<Map<String, Store>> stores;

  public final ClickHouseDataSource dataSource;

  public final ClickHouseNodes servers;

  public ClickHouseDatastore(String jdbc) {
    try {
      this.servers = ClickHouseJdbcUrlParser.parse(jdbc, null).getNodes();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    this.dataSource = newDataSource(jdbc, null);
    this.stores = Suppliers.memoize(
            () -> getTableNames(this.dataSource)
                    .stream()
                    .collect(() -> new HashMap<>(),
                            (map, table) -> map.put(table, new Store(table, getFields(this.dataSource, table))),
                            (x, y) -> {
                            }));
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
      ResultSet tables = metaData.getTables(null, (String) ClickHouseDefaults.DATABASE.getDefaultValue(), null, null);

      Set<String> tableNames = new HashSet<>();
      while (tables.next()) {
        tableNames.add((String) tables.getObject("TABLE_NAME"));
      }

      return tableNames;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<TypedField> getFields(ClickHouseDataSource dataSource, String table) {
    try {
      DatabaseMetaData metaData = dataSource.getConnection().getMetaData();
      ResultSet columns = metaData.getColumns(null, (String) ClickHouseDefaults.DATABASE.getDefaultValue(), table, null);

      List<TypedField> fields = new ArrayList<>();
      while (columns.next()) {
        String columnName = (String) columns.getObject("COLUMN_NAME");
        String typeName = (String) columns.getObject("TYPE_NAME");
        ClickHouseColumn column = ClickHouseColumn.of("", typeName);
        fields.add(new TypedField(table, columnName, ClickHouseUtil.clickHouseTypeToClass(column.getDataType())));
      }

      return fields;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
