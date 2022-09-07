package me.paulbares.query.database;

import me.paulbares.SparkDatastore;
import me.paulbares.query.Table;

import java.util.List;

import static me.paulbares.query.database.SQLTranslator.virtualTableStatementWhereNotExists;
import static me.paulbares.query.database.SparkQueryEngine.getResults;

public class SparkDeltaQueryEngine extends ADeltaQueryEngine<SparkDatastore> {

  public SparkDeltaQueryEngine(SparkDatastore datastore) {
    super(datastore);
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query) {
    List<String> keys = List.of("ean", "category"); // FIXME should not be hardcoded but in the query.
    List<String> listOfScenarios = getListOfScenarios(query.table.name);// does it have scenario?

    String sql = SQLTranslator.translate(query, null, this.fieldSupplier, DefaultQueryRewriter.INSTANCE, (qr, tableName) -> {
      String newTableName = qr.tableName(tableName);
      if (listOfScenarios.isEmpty()) {
        return newTableName;
      } else {
        String virtualTable = virtualTableStatementWhereNotExists(query.table.name, listOfScenarios, keys, qr);
        return "(" + virtualTable + ") as vt_" + newTableName;
      }
    });
    return getResults(sql, this.datastore.spark, query);
  }
}
