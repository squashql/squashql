package io.squashql.query.database;

import com.clickhouse.client.*;
import io.squashql.ClickHouseDatastore;
import io.squashql.ClickHouseUtil;
import io.squashql.query.ColumnarTable;
import io.squashql.query.Header;
import io.squashql.query.Table;
import io.squashql.store.Field;
import java.util.HashSet;
import org.eclipse.collections.api.tuple.Pair;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class ClickHouseQueryEngine extends AQueryEngine<ClickHouseDatastore> {

  /**
   * <a href="https://clickhouse.com/docs/en/sql-reference/aggregate-functions/reference/">aggregate functions</a>
   * NOTE: there is more but only a subset is proposed here.
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

  public ClickHouseQueryEngine(ClickHouseDatastore datastore) {
    super(datastore, new ClickHouseQueryRewriter());
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query, String sql) {
    ClickHouseNode server = ClickHouseNode.builder()
            .host(this.datastore.dataSource.getHost())
            .port(ClickHouseProtocol.HTTP, this.datastore.dataSource.getPort())
            .build();

    try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
            ClickHouseResponse response = client.connect(server)
                    .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                    .query(sql)
                    .execute()
                    .get()) {
      Pair<List<Header>, List<List<Object>>> result = transform(
              query,
              response.getColumns(),
              (column, name) -> new Field(name, ClickHouseUtil.clickHouseTypeToClass(column.getDataType())),
              response.records().iterator(),
              (i, r) -> r.getValue(i).asObject(),
              this.queryRewriter);
      return new ColumnarTable(
              result.getOne(),
              new HashSet<>(query.measures),
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
    public String select(String select) {
      return SqlUtils.backtickEscape(select);
    }

    @Override
    public String rollup(String rollup) {
      return SqlUtils.backtickEscape(rollup);
    }

    @Override
    public String measureAlias(String alias) {
      return SqlUtils.backtickEscape(alias);
    }

    @Override
    public String groupingAlias(String field) {
      return SqlUtils.backtickEscape(QueryRewriter.super.groupingAlias(field));
    }

    @Override
    public boolean usePartialRollupSyntax() {
      // Not supported as of now: https://github.com/ClickHouse/ClickHouse/issues/322#issuecomment-615087004
      // Tested with version https://github.com/ClickHouse/ClickHouse/tree/v22.10.2.11-stable
      return false;
    }

    @Override
    public boolean useGroupingFunction() {
      return true;
    }
  }
}
