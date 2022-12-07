package me.paulbares.transaction;

import com.google.cloud.bigquery.*;
import lombok.extern.slf4j.Slf4j;
import me.paulbares.BigQueryDatastore;
import me.paulbares.BigQueryUtil;
import me.paulbares.store.Field;
import org.eclipse.collections.impl.list.immutable.ImmutableListFactoryImpl;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class BigQueryTransactionManager implements TransactionManager {

  // 1, 2, 4, 8, 16
  private static final int MAX_SLEEPS = 7;

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
          Object o = tuple[i];
          if (o != null && (o.getClass().equals(LocalDate.class) || o.getClass().equals(LocalDateTime.class))) {
            o = o.toString();
          }
          m.put(name, o);
        } else {
          m.put(name, scenario);
        }
      }
      list.add(InsertAllRequest.RowToInsert.of(m));
    }

    TableId tableId = TableId.of(this.datasetName, store);
    Table table = this.bigquery.getTable(tableId);

    int sleepTime = 1;// Start at 1 s.
    int attempt = 0;
    while (true) {
      // table creation is eventually consistent, try several time to insert it.
      // https://stackoverflow.com/questions/73544951/no-table-found-for-new-bigquery-table
      // Still issues even after this retry.
      try {
        InsertAllResponse response = table.insert(list);
        if (response.hasErrors()) {
          for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
            System.out.println("Response error: \n" + entry.getValue());
          }
          throw new RuntimeException("error while inserting rows, see above");
        }
        return;
      } catch (BigQueryException exception) {
        if (exception.getCode() == 404 && exception.getReason().equals("notFound")) {
          try {
            Thread.sleep(sleepTime * 1000);
          } catch (InterruptedException e) {
            log.error("", e);
            Thread.currentThread().interrupt();
          }
          if (attempt < MAX_SLEEPS) {
            sleepTime <<= 1;
            attempt++;
            log.info("Table not found, retry " + attempt);
          } else {
            log.info("Table not found after " + MAX_SLEEPS + " attempts. Abort.");
            throw exception;
          }
        } else {
          throw exception;
        }
      }
    }
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
