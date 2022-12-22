package me.paulbares.query.database;

import com.clickhouse.client.*;
import com.clickhouse.jdbc.ClickHouseDataSource;
import me.paulbares.ClickHouseDatastore;
import me.paulbares.ClickHouseUtil;
import me.paulbares.query.ColumnarTable;
import me.paulbares.query.QueryExecutor;
import me.paulbares.query.Table;
import me.paulbares.store.Field;
import org.eclipse.collections.api.tuple.Pair;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public class ClickHouseQueryEngine extends AQueryEngine<ClickHouseDatastore> {

  /**
   * https://clickhouse.com/docs/en/sql-reference/aggregate-functions/reference/
   * NOTE there is more but only a subset is proposed here.
   */
  public static final List<String> SUPPORTED_AGGREGATION_FUNCTIONS = List.of(
          "count",
          "min",
          "max",
          "sum",
          "avg",
          "any",
          "stddevPop",
          "stddevSamp",
          "varPop",
          "varSamp",
          "covarPop",
          "covarSamp");

  protected final QueryRewriter queryRewriter;

  public ClickHouseQueryEngine(ClickHouseDatastore datastore) {
    super(datastore);
    this.queryRewriter = new ClickHouseQueryRewriter();
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query) {
    String sql = SQLTranslator.translate(query, QueryExecutor.withFallback(this.fieldSupplier, String.class),
            this.queryRewriter, QueryRewriter::tableName);
    return getResults(sql, this.datastore.dataSource, query, this.queryRewriter);
  }

  static Table getResults(String sql, ClickHouseDataSource dataSource, DatabaseQuery query, QueryRewriter queryRewriter) {
    // connect to localhost, use default port of the preferred protocol
    ClickHouseNode server = ClickHouseNode.builder()
            .host(dataSource.getHost())
            .port(dataSource.getPort())
            .build();

    try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
         ClickHouseResponse response = client.connect(server)
                 .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                 .query(sql)
                 .execute()
                 .get()) {
      Pair<List<Field>, List<List<Object>>> result = transform(
              query,
              response.getColumns(),
              (column, name) -> new Field(name, ClickHouseUtil.clickHouseTypeToClass(column.getDataType())),
              response.records().iterator(),
              (i, r) -> r.getValue(i).asObject(),
              queryRewriter);
      return new ColumnarTable(
              result.getOne(),
              query.measures,
              IntStream.range(query.select.size(), query.select.size() + query.measures.size()).toArray(),
              IntStream.range(0, query.select.size()).toArray(),
              result.getTwo());
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<String> supportedAggregationFunctions() {
    return SUPPORTED_AGGREGATION_FUNCTIONS;
  }

  static class ClickHouseQueryRewriter implements QueryRewriter {

    @Override
    public String fieldName(String field) {
      return SqlUtils.backtickEscape(field);
    }

    @Override
    public String measureAlias(String alias) {
      return SqlUtils.backtickEscape(alias);
    }

    @Override
    public String rollup(String rollup) {
      return SqlUtils.backtickEscape(rollup);
    }

    @Override
    public String groupingAlias(String field) {
      return SqlUtils.backtickEscape(QueryRewriter.super.groupingAlias(field));
    }

    @Override
    public boolean doesSupportPartialRollup() {
      // Not supported as of now: https://github.com/ClickHouse/ClickHouse/issues/322#issuecomment-615087004
      // Tested with version https://github.com/ClickHouse/ClickHouse/tree/v22.10.2.11-stable
      return false;
    }
  }
}
