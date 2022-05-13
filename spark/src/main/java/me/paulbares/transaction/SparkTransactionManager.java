package me.paulbares.transaction;

import me.paulbares.SparkDatastore;
import me.paulbares.SparkStore;
import me.paulbares.store.Field;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class SparkTransactionManager implements TransactionManager {

  protected final SparkSession spark;

  protected final SparkDatastore datastore;

  public SparkTransactionManager(SparkSession spark, SparkDatastore datastore) {
    this.spark = spark;
    this.datastore = datastore;
  }

  public SparkDatastore createTable(String table, List<Field> fields) {
    return null; // TODO
  }

  @Override
  public void load(String scenario, String store, List<Object[]> tuples) {
    SparkStore sparkStore = this.datastore.storesByName().get(store);
    List<Row> rows = tuples.stream().map(RowFactory::create).toList();
    Dataset<Row> dataFrame = this.spark.createDataFrame(rows, sparkStore.getBaseSchema());// to load pojo
//    Dataset<Row> dataset = addAdditionalColumns(scenario, dataFrame);
//    save(scenario, dataset);
  }

//  protected Dataset<Row> addAdditionalColumns(String scenario, Dataset<Row> dataFrame) {
//    for (Column column : this.columns) {
//      dataFrame = dataFrame.withColumn(column.named().name(), column);
//    }
//    return dataFrame.withColumn(scenarioFieldName(), functions.lit(scenario));
//  }

  @Override
  public void loadCsv(String scenario, String store, String path, String delimiter, boolean header) {

  }
}
