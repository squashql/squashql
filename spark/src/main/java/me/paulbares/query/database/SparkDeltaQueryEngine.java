package me.paulbares.query.database;

import me.paulbares.SparkDatastore;
import me.paulbares.query.Table;
import me.paulbares.query.database.SparkQueryEngine.SparkQueryRewriter;

import java.util.List;

import static me.paulbares.query.database.SQLTranslator.virtualTableStatementWhereNotExists;
import static me.paulbares.query.database.SparkQueryEngine.getResults;

public class SparkDeltaQueryEngine extends ADeltaQueryEngine<SparkDatastore> {

  private final QueryRewriter queryRewriter;

  public SparkDeltaQueryEngine(SparkDatastore datastore) {
    super(datastore);
    this.queryRewriter = new SparkQueryRewriter();
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query) {
    List<String> keys = List.of("ean", "category"); // FIXME should not be hardcoded but in the query.
    var tableTransformer = new TableTransformer(this.datastore, keys) {
      @Override
      protected String virtualTableStatement(String baseTableName, List<String> scenarios, List<String> columnKeys, QueryRewriter qr) {
        return virtualTableStatementWhereNotExists(baseTableName, scenarios, this.keys, qr);
      }
    };
    String sql = SQLTranslator.translate(query,
            this.fieldSupplier,
            this.queryRewriter,
            tableTransformer);
    return getResults(sql, this.datastore.spark, query, this.queryRewriter);
  }

  @Override
  public List<String> supportedAggregationFunctions() {
    return SparkQueryEngine.SUPPORTED_AGGREGATION_FUNCTIONS;
  }
}
