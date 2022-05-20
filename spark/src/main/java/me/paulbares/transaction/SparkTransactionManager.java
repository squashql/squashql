package me.paulbares.transaction;

import me.paulbares.SparkDatastore;
import me.paulbares.SparkStore;
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
//    Dataset<Row> dataFrame = this.spark.emptyDataFrame();
//    StructField[] sparkFields = dataFrame.schema().fields();
//    List<Field> newFields = new ArrayList<>(fields);
//    newFields.addAll(Arrays.stream(sparkFields)
//            .map(f -> new Field(f.name(), SparkStore.datatypeToClass(f.dataType())))
//            .toList());
//    SparkStore store = new SparkStore(table, fields);
//    try {
//      String uriSt = this.spark.catalog().getDatabase("default").locationUri();
//      URI uri = new URI(uriSt);
//      Path path = Paths.get(uri);
//      File dir = Paths.get(path.toString(), table).toFile();
//      if (dir.isDirectory() && dir.exists()) {
//        FileUtils.deleteDirectory(dir);
//      }
//    } catch (AnalysisException | URISyntaxException | IOException e) {
//      throw new RuntimeException(e);
//    }
//    this.spark.conf().set("spark.sql.caseSensitive", "true"); // without it, table names are lowercase.
//    this.spark.createDataFrame(Collections.EMPTY_LIST, store.getSchema())
//            .persist(StorageLevel.MEMORY_ONLY())
//            .write()
//            .mode(SaveMode.Append)
//            .saveAsTable(table);

    StructType schema = SparkStore.createSchema(ImmutableListFactoryImpl.INSTANCE
            .ofAll(fields)
            .newWith(new Field(SparkStore.getScenarioName(table), String.class))
            .castToList());
    this.spark.conf().set("spark.sql.caseSensitive", String.valueOf(true)); // without it, table names are lowercase.
    this.spark
            .createDataFrame(Collections.emptyList(), schema)
            .createOrReplaceTempView(table);
//    Collection<String> tableNames = SparkDatastore.getTableNames(spark);
//    List<Field> my_temp_table = SparkDatastore.getFields(spark, "my_temp_table");
//    spark.sql("drop table if exists " + table);
//    spark.sql("create table " + table + " as select * from my_temp_table LOCATION /tmp/zob");
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
            SparkStore.createSchema(SparkDatastore.getFields(this.spark, store)));// to load pojo
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
    String scenarioName = SparkStore.getScenarioName(store);
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
            .withColumn(SparkStore.getScenarioName(store), functions.lit(scenario));

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
