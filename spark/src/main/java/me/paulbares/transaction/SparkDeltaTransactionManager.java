package me.paulbares.transaction;

import me.paulbares.SparkDatastore;
import me.paulbares.SparkUtil;
import me.paulbares.store.TypedField;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static me.paulbares.transaction.TransactionManager.scenarioStoreName;

public class SparkDeltaTransactionManager extends SparkTransactionManager {

  private Set<String> storeAlreadyCreated = new HashSet<>();

  public SparkDeltaTransactionManager(SparkSession spark) {
    super(spark);
  }

  @Override
  public void createTemporaryTable(String table, List<TypedField> fields) {
    createTemporaryTable(this.spark, table, fields, false);
  }

  @Override
  public void load(String scenario, String store, List<Object[]> tuples) {
    ensureScenarioTableIsPresent(store, scenario);
    List<Row> rows = tuples.stream().map(RowFactory::create).toList();
    Dataset<Row> dataFrame = this.spark.createDataFrame(
            rows,
            SparkUtil.createSchema(SparkDatastore.getFields(this.spark, store)));// to load pojo
    appendDataset(this.spark, scenarioStoreName(store, scenario), dataFrame);
  }

  private void ensureScenarioTableIsPresent(String store, String scenario) {
    String storeName = scenarioStoreName(store, scenario);
    Collection<String> tableNames = SparkDatastore.getTableNames(this.spark);
    boolean found = tableNames.stream().anyMatch(f -> f.equals(storeName));
    if (this.storeAlreadyCreated.add(storeName) || !found) {
      // Create if not found
      createTemporaryTable(storeName, SparkDatastore.getFields(this.spark, store));
    }
  }

  @Override
  public void loadCsv(String scenario, String store, String path, String delimiter, boolean header) {
    throw new RuntimeException("not implemented yet");
  }
}
