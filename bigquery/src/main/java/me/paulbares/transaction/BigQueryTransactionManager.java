package me.paulbares.transaction;

import com.google.cloud.bigquery.*;
import me.paulbares.BigQueryDatastore;
import me.paulbares.BigQueryUtil;
import me.paulbares.store.Field;
import org.eclipse.collections.impl.list.immutable.ImmutableListFactoryImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigQueryTransactionManager implements TransactionManager {

  final BigQuery bigquery;
  final String datasetName;

  public BigQueryTransactionManager(BigQuery bigquery, String datasetName) {
    this.bigquery = bigquery;
    this.datasetName = datasetName;
  }

  public BigQuery getBigQuery() {
    return this.bigquery;
  }

  public void dropAndCreateInMemoryTable(String tableName, List<Field> fields) {
    List<Field> list = ImmutableListFactoryImpl.INSTANCE
            .ofAll(fields)
            .newWith(new Field(SCENARIO_FIELD_NAME, String.class))
            .castToList();

    TableId tableId = TableId.of(this.datasetName, tableName);

    List<com.google.cloud.bigquery.Field> fieldList = list.stream()
            .map(f -> com.google.cloud.bigquery.Field.of(f.name(), BigQueryUtil.classToBigQueryType(f.type())))
            .toList();
    // Table schema definition
    Schema schema = Schema.of(fieldList);
    TableDefinition tableDefinition = StandardTableDefinition.of(schema);
    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

    try {
      this.bigquery.create(tableInfo);
    } catch (BigQueryException e) {
      if (e.getCode() == 409 && e.getReason().equals("duplicate")) {
        this.bigquery.delete(tableId);
        this.bigquery.create(tableInfo);
      } else {
        throw e;
      }
    }
  }

  @Override
  public void load(String scenario, String store, List<Object[]> tuples) {
    // Check the table contains a column scenario.
    ensureScenarioColumnIsPresent(store);

    List<Field> fields = BigQueryDatastore.getFields(this.bigquery, this.datasetName, store);
    List<InsertAllRequest.RowToInsert> list = new ArrayList<>();
    for (Object[] tuple : tuples) {
      Map<String, Object> m = new HashMap<>();
      for (int i = 0; i < fields.size(); i++) {
        String name = fields.get(i).name();
        if (!name.equals(SCENARIO_FIELD_NAME)) {
          m.put(name, tuple[i]);
        } else {
          m.put(name, scenario);
        }
      }
      list.add(InsertAllRequest.RowToInsert.of(m));
    }

    TableId tableId = TableId.of(this.datasetName, store);
    Table table = this.bigquery.getTable(tableId);
    table.insert(list);
    // FIXME issue with Free Tier
    // com.google.cloud.bigquery.BigQueryException: Access Denied: BigQuery BigQuery: Streaming insert is not allowed in the free tier
    // similar issue when trying sql inster queries:
    // "message" : "Billing has not been enabled for this project. DML queries are not allowed in the free tier. Set up a billing account to remove this restriction.",
  }

  private void ensureScenarioColumnIsPresent(String store) {
    List<Field> fields = BigQueryDatastore.getFields(this.bigquery, this.datasetName, store);
    boolean found = fields.stream().anyMatch(f -> f.name().equals(SCENARIO_FIELD_NAME));
    if (!found) {
      throw new RuntimeException(String.format("%s field not found", SCENARIO_FIELD_NAME));
    }
  }

  @Override
  public void loadCsv(String scenario, String store, String path, String delimiter, boolean header) {
    throw new RuntimeException("not impl. yet");
  }
}
