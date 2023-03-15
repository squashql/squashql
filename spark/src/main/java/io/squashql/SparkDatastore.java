package io.squashql;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.base.Suppliers;
import io.squashql.store.Datastore;
import io.squashql.store.Field;
import io.squashql.store.Store;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalog.Column;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.types.DataType;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;

public class SparkDatastore implements Datastore {

  static {
    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.INFO);
  }

  public final Supplier<Map<String, Store>> stores;

  public final SparkSession spark;

  public SparkDatastore() {
    this(SparkSession
            .builder()
            .appName("Java Spark SQL Example")
            .config("spark.master", "local")
            .getOrCreate());
  }

  public SparkDatastore(SparkSession sparkSession) {
    this.spark = sparkSession;
    this.stores = Suppliers.memoize(() -> {
      Map<String, Store> r = new HashMap<>();
      getTableNames(this.spark).forEach(table -> r.put(table, new Store(table, getFields(this.spark, table))));
      return r;
    });
  }

  @Override
  public Map<String, Store> storesByName() {
    return this.stores.get();
  }

  public Dataset<Row> get(String storeName) {
    return this.spark.table(storeName);
  }

  public static Collection<String> getTableNames(SparkSession spark) {
    try {
      Dataset<Table> tables = spark.catalog().listTables("default");
      Set<String> tableNames = new HashSet<>();
      Iterator<Table> tableIterator = tables.toLocalIterator();
      while (tableIterator.hasNext()) {
        tableNames.add(tableIterator.next().name());
      }
      return tableNames;
    } catch (AnalysisException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<Field> getFields(SparkSession spark, String tableName) {
    try {
      Catalog catalog = spark.catalog();
      Table table = catalog.getTable(tableName);
      Dataset<Column> columns = table.isTemporary()
              ? catalog.listColumns(tableName)
              : catalog.listColumns("default", tableName);
      List<Field> fields = new ArrayList<>();
      Iterator<Column> columnIterator = columns.toLocalIterator();
      while (columnIterator.hasNext()) {
        Column column = columnIterator.next();
        fields.add(new Field(tableName, column.name(), SparkUtil.datatypeToClass(DataType.fromDDL(column.dataType()))));
      }
      return fields;
    } catch (AnalysisException e) {
      throw new RuntimeException(e);
    }
  }
}
