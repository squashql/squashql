package me.paulbares;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Column;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SparkDatastore implements Datastore {

  static {
    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.INFO);
  }

  public final Map<String, SparkStore> stores = new HashMap<>();

  public final SparkSession spark;

  public SparkDatastore(List<SparkStore> stores) {  // FIXME remote stores
    this(SparkSession
                    .builder()
                    .appName("Java Spark SQL Example")
                    .config("spark.master", "local")
                    .getOrCreate(),
            stores);
  }

  public SparkDatastore(SparkSession sparkSession, List<SparkStore> stores) { // FIXME remote stores
    this.spark = sparkSession;
    for (SparkStore store : stores) {
      this.stores.put(store.name(), store);
    }
  }

  @Override
  public Map<String, SparkStore> storesByName() {
    return this.stores; // FIXME lazy load See ClickHouse
  }

  public Dataset<Row> get(String storeName) {
    return this.spark.table(storeName);
  }

  public void persist(StorageLevel storageLevel) {
//    this.stores.forEach((name, store) -> store.persist(storageLevel));
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
      Dataset<Column> columns = spark.catalog().listColumns("default", tableName);
      List<Field> fields = new ArrayList<>();
      Iterator<Column> columnIterator = columns.toLocalIterator();
      while (columnIterator.hasNext()) {
        Column column = columnIterator.next();
        fields.add(new Field(column.name(), SparkStore.datatypeToClass(DataType.fromDDL(column.dataType()))));
      }
      return fields;
    } catch (AnalysisException e) {
      throw new RuntimeException(e);
    }
  }
}
