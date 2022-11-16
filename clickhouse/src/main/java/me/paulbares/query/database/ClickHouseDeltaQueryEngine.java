package me.paulbares.query.database;

import me.paulbares.ClickHouseDatastore;
import me.paulbares.query.Table;

import java.util.List;

import static me.paulbares.query.database.ClickHouseQueryEngine.getResults;
import static me.paulbares.query.database.SQLTranslator.virtualTableStatementWhereNotIn;

public class ClickHouseDeltaQueryEngine extends ADeltaQueryEngine<ClickHouseDatastore> {

  public ClickHouseDeltaQueryEngine(ClickHouseDatastore datastore) {
    super(datastore);
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query) {
    List<String> keys = List.of("ean", "category"); // FIXME should not be hardcoded but in the query.
    var tableTransformer = new TableTransformer(this.datastore, keys) {
      @Override
      protected String virtualTableStatement(String baseTableName, List<String> scenarios, List<String> columnKeys, QueryRewriter qr) {
        return virtualTableStatementWhereNotIn(baseTableName, scenarios, keys, qr);
      }
    };
    String sql = SQLTranslator.translate(query,
            this.fieldSupplier,
            new ClickHouseQueryEngine.ClickHouseQueryRewriter(),
            tableTransformer);
    return getResults(sql, this.datastore.dataSource, query);
  }
}
