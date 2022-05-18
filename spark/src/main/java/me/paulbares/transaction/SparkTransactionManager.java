package me.paulbares.transaction;

import me.paulbares.SparkDatastore;
import me.paulbares.SparkStore;
import me.paulbares.store.Field;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.storage.StorageLevel;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SparkTransactionManager implements TransactionManager {

  protected final SparkSession spark;

  protected final SparkDatastore datastore;

  public SparkTransactionManager(SparkSession spark, SparkDatastore datastore) {
    this.spark = spark;
    this.datastore = datastore;
  }

  public SparkStore createTable(String table, List<Field> fields, Column... columns) {
    Dataset<Row> dataFrame = this.spark.emptyDataFrame();
    for (Column column : columns) {
      dataFrame = dataFrame.withColumn(column.named().name(), column);
    }
    StructField[] sparkFields = dataFrame.schema().fields();
    List<Field> newFields = new ArrayList<>(fields);
    newFields.addAll(Arrays.stream(sparkFields)
            .map(f -> new Field(f.name(), SparkStore.datatypeToClass(f.dataType())))
            .toList());
    SparkStore store = new SparkStore(table, fields);
    try {
      String uriSt = this.spark.catalog().getDatabase("default").locationUri();
      URI uri = new URI(uriSt);
      Path path = Paths.get(uri);
      File dir = Paths.get(path.toString(), table.toLowerCase()).toFile();
      if (dir.isDirectory() && dir.exists()) {
        FileUtils.deleteDirectory(dir);
      }
    } catch (AnalysisException | URISyntaxException | IOException e) {
      throw new RuntimeException(e);
    }
    this.spark.createDataFrame(Collections.EMPTY_LIST, store.getSchema())
            .persist(StorageLevel.MEMORY_ONLY())
            .write()
            .mode(SaveMode.Append)
            .saveAsTable(table);
    return store;
  }

  @Override
  public void load(String scenario, String store, List<Object[]> tuples) {
    // Check the table contains a column scenario.
    ensureScenarioColumnIsPresent(store);
    List<Row> rows = new ArrayList<>();
    for (Object[] tuple : tuples) {
      Object[] copy = Arrays.copyOf(tuple, tuple.length + 1);
      copy[copy.length - 1] = scenario;
      rows.add(RowFactory.create(copy));
    }
    Dataset<Row> dataFrame = this.spark.createDataFrame(rows, SparkStore.createSchema(SparkDatastore.getFields(this.spark, store)));// to load pojo
    try {
      dataFrame.write().mode(SaveMode.Append).saveAsTable(store);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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
    throw new RuntimeException("TODO");
  }
}
