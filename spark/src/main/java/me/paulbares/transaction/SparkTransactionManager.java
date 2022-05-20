package me.paulbares.transaction;

import me.paulbares.SparkDatastore;
import me.paulbares.SparkUtil;
import me.paulbares.store.Field;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.eclipse.collections.impl.list.immutable.ImmutableListFactoryImpl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SparkTransactionManager implements TransactionManager {

  protected final SparkSession spark;

  public SparkTransactionManager(SparkSession spark) {
    this.spark = spark;
  }

  public void createTemporaryTable(String table, List<Field> fields) {
    StructType schema = SparkUtil.createSchema(ImmutableListFactoryImpl.INSTANCE
            .ofAll(fields)
            .newWith(new Field(SparkUtil.getScenarioName(table), String.class))
            .castToList());
    this.spark.conf().set("spark.sql.caseSensitive", String.valueOf(true)); // without it, table names are lowercase.
    this.spark
            .createDataFrame(Collections.emptyList(), schema)
            .createOrReplaceTempView(table);
  }

  @Override
  public void load(String scenario, String store, List<Object[]> tuples) {
    // Check the table contains a column scenario.
    ensureScenarioColumnIsPresent(store);

    List<Row> rows = tuples.stream().map(tuple -> {
      Object[] copy = Arrays.copyOf(tuple, tuple.length + 1);
      copy[copy.length - 1] = scenario;
      return RowFactory.create(copy);
    }).toList();

    Dataset<Row> dataFrame = this.spark.createDataFrame(
            rows,
            SparkUtil.createSchema(SparkDatastore.getFields(this.spark, store)));// to load pojo
    appendDataset(store, dataFrame);
  }

  private void appendDataset(String store, Dataset<Row> dataset) {
    String viewName = "tmp_" + store;
    this.spark.sql("ALTER VIEW " + store + " RENAME TO " + viewName);
    Dataset<Row> table = this.spark.table(viewName);
    Dataset<Row> union = table.union(dataset);
    union.createOrReplaceTempView(store);
    this.spark.catalog().dropTempView(viewName);
  }

  private void ensureScenarioColumnIsPresent(String store) {
    List<Field> fields = SparkDatastore.getFields(this.spark, store);
    String scenarioName = SparkUtil.getScenarioName(store);
    boolean found = fields.stream().anyMatch(f -> f.name().equals(scenarioName));
    if (!found) {
      throw new RuntimeException(String.format("%s field not found", scenarioName));
    }
  }

  @Override
  public void loadCsv(String scenario, String store, String path, String delimiter, boolean header) {
    Dataset<Row> dataFrame = this.spark.read()
            .option("delimiter", delimiter)
            .option("header", true)
            .csv(path)
            .withColumn(SparkUtil.getScenarioName(store), functions.lit(scenario));

    Table table = null;
    try {
      table = this.spark.catalog().getTable(store);
    } catch (AnalysisException e) {
      // Swallow, table not found
    }

    if (table == null) {
      this.spark.conf().set("spark.sql.caseSensitive", String.valueOf(true)); // without it, table names are lowercase.
      dataFrame.createOrReplaceTempView(store);
    } else {
      appendDataset(store, dataFrame);
    }
  }
}
