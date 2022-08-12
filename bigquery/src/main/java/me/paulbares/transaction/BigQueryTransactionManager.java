package me.paulbares.transaction;

import com.google.cloud.bigquery.*;
import me.paulbares.BigQueryUtil;
import me.paulbares.store.Field;
import org.eclipse.collections.impl.list.immutable.ImmutableListFactoryImpl;

import java.util.List;
import java.util.stream.IntStream;

public class BigQueryTransactionManager implements TransactionManager {

  final BigQuery bigquery;

  public BigQueryTransactionManager(BigQuery bigquery) {
    this.bigquery = bigquery;
  }

  public BigQuery getBigQuery() {
    return this.bigquery;
  }

  public void dropAndCreateInMemoryTable(String datasetName, String tableName, List<Field> fields) {
    List<Field> list = ImmutableListFactoryImpl.INSTANCE
            .ofAll(fields)
            .newWith(new Field(SCENARIO_FIELD_NAME, String.class))
            .castToList();

    TableId tableId = TableId.of(datasetName, tableName);

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
    String join = String.join(",", IntStream.range(0, tuples.get(0).length + 1).mapToObj(i -> "?").toList());
    String pattern = "insert into " + store + " values(" + join + ")";
//    try (ClickHouseConnection conn = this.clickHouseDataSource.getConnection();
//         PreparedStatement stmt = conn.prepareStatement(pattern)) {
//
//      for (Object[] tuple : tuples) {
//        for (int i = 0; i < tuple.length; i++) {
//          stmt.setObject(i + 1, tuple[i]);
//        }
//        stmt.setObject(tuple.length + 1, scenario);
//        stmt.addBatch();
//      }
//      stmt.executeBatch();
//    } catch (SQLException e) {
//      throw new RuntimeException(e);
//    }
  }

  private void ensureScenarioColumnIsPresent(String store) {
//    List<Field> fields = ClickHouseDatastore.getFields(this.clickHouseDataSource, store);
//    boolean found = fields.stream().anyMatch(f -> f.name().equals(SCENARIO_FIELD_NAME));
//    if (!found) {
//      throw new RuntimeException(String.format("%s field not found", SCENARIO_FIELD_NAME));
//    }
  }

  @Override
  public void loadCsv(String scenario, String store, String path, String delimiter, boolean header) {
//    ClickHouseNode clickHouseNode = ClickHouseNode.of(this.clickHouseDataSource.getHost(),
//            ClickHouseProtocol.HTTP,
//            this.clickHouseDataSource.getPort(),
//            null);
//
//    try {
//      CompletableFuture<ClickHouseResponseSummary> load = ClickHouseClient.load(
//              clickHouseNode,
//              store,
//              header ? ClickHouseFormat.CSVWithNames : ClickHouseFormat.CSV,
//              ClickHouseCompression.LZ4,
//              path);
//      load.get();
//    } catch (FileNotFoundException | InterruptedException | ExecutionException e) {
//      throw new RuntimeException(e);
//    }
  }
}
