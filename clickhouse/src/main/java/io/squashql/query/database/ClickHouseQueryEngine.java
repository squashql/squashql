package io.squashql.query.database;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNodes;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseValue;
import io.squashql.ClickHouseDatastore;
import io.squashql.ClickHouseUtil;
import io.squashql.query.Header;
import io.squashql.table.ColumnarTable;
import io.squashql.table.RowTable;
import io.squashql.table.Table;
import org.eclipse.collections.api.tuple.Pair;

import java.util.Arrays;
import java.util.HashSet;
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

  protected final ClickHouseNodes nodes;

  public ClickHouseQueryEngine(ClickHouseDatastore datastore) {
    super(datastore);
    this.nodes = datastore.servers;
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query, String sql) {
    try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
         ClickHouseResponse response = client.read(this.nodes)
                 .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                 .query(sql)
                 .execute()
                 .get()) {
      Pair<List<Header>, List<List<Object>>> result = transformToColumnFormat(
              query,
              response.getColumns(),
              (column, name) -> ClickHouseUtil.clickHouseTypeToClass(column.getDataType()),
              response.records().iterator(),
              (index, r) -> getValue(r, index, response.getColumns()));
      return new ColumnarTable(
              result.getOne(),
              new HashSet<>(query.measures),
              result.getTwo());
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Table executeRawSql(String sql) {
    try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
         ClickHouseResponse response = client.read(this.nodes)
                 .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                 .query(sql)
                 .execute()
                 .get()) {
      Pair<List<Header>, List<List<Object>>> result = transformToRowFormat(
              response.getColumns(),
              column -> column.getColumnName(),
              column -> ClickHouseUtil.clickHouseTypeToClass(column.getDataType()),
              response.records().iterator(),
              (i, r) -> getValue(r, i, response.getColumns()));
      return new RowTable(result.getOne(), result.getTwo());
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the value with the correct type. We can't directly call {@link ClickHouseValue#asObject()} because in some cases
   * it could return an object from ClickHouse like a {@link com.clickhouse.data.value.UnsignedLong}.
   * See {@code com.clickhouse.data.value.ClickHouseLongValue$UnsignedLong}.
   */
  public static Object getValue(ClickHouseRecord record, int index, List<ClickHouseColumn> columns) {
    ClickHouseValue fieldValue = record.getValue(index);
    ClickHouseColumn column = columns.get(index);
    Object object = fieldValue.asObject();
    if (object == null) {
      // There is a check in BQ client when trying to access the value and throw if null.
      return null;
    }
    return switch (column.getDataType()) {
      case Bool -> fieldValue.asBoolean();
      case Date -> fieldValue.asDate();
      case Int8, UInt32, Int32, UInt16, Int16, UInt8 -> fieldValue.asInteger();
      case Int64, UInt64 -> fieldValue.asLong();
      case Float32 -> fieldValue.asFloat();
      case Float64 -> fieldValue.asDouble();
      case String, FixedString -> fieldValue.asString();
      case Array -> Arrays.stream(fieldValue.asArray()).toList();
      default -> throw new RuntimeException("Unexpected type " + column.getDataType());
    };
  }

  @Override
  public List<String> supportedAggregationFunctions() {
    return SUPPORTED_AGGREGATION_FUNCTIONS;
  }

  @Override
  public QueryRewriter queryRewriter(DatabaseQuery query) {
    return new ClickHouseQueryRewriter(query);
  }

  static class ClickHouseQueryRewriter implements QueryRewriter {

    private final DatabaseQuery query;

    ClickHouseQueryRewriter(DatabaseQuery query) {
      this.query = query;
    }

    @Override
    public DatabaseQuery query() {
      return this.query;
    }

    @Override
    public String fieldName(String field) {
      return SqlUtils.backtickEscape(field);
    }

    @Override
    public String escapeAlias(String alias) {
      return SqlUtils.backtickEscape(alias);
    }

    @Override
    public boolean usePartialRollupSyntax() {
      // Not supported as of now: https://github.com/ClickHouse/ClickHouse/issues/322#issuecomment-615087004
      // Tested with version https://github.com/ClickHouse/ClickHouse/tree/v22.10.2.11-stable
      return false;
    }
  }
}
