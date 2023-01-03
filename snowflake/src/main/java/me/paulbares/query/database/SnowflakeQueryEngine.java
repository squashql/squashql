package me.paulbares.query.database;

import java.sql.Types;
import me.paulbares.SnowflakeDatastore;
import me.paulbares.SnowflakeUtil;
import me.paulbares.query.ColumnarTable;
import me.paulbares.query.Table;
import me.paulbares.store.Field;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class SnowflakeQueryEngine extends AQueryEngine<SnowflakeDatastore> {

  /**
   * https://docs.snowflake.com/en/sql-reference/functions-aggregation.html
   * NOTE there is more but only a subset is proposed here.
   */
  public static final List<String> SUPPORTED_AGGREGATION_FUNCTIONS = List.of(
          "ANY_VALUE",
          "AVG",
          "CORR",
          "COUNT",
          "COUNT_IF",
          "COVAR_POP",
          "COVAR_SAMP",
          "LISTAGG",
          "MAX",
          "MEDIAN",
          "MIN",
          "MODE",
          "STDDEV_POP",
          "STDDEV_SAMP",
          "SUM",
          "VAR_POP",
          "VAR_SAMP");

  private final QueryRewriter queryRewriter;

  public SnowflakeQueryEngine(SnowflakeDatastore datastore) {
    super(datastore);
    this.queryRewriter = new SnowflakeQueryRewriter();
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query) {
    String sql = SQLTranslator.translate(query, this.fieldSupplier, this.queryRewriter, QueryRewriter::tableName);
    try (Statement snowflakeStatement = this.datastore.getConnection().createStatement()) {
      ResultSet tableResult = snowflakeStatement.executeQuery(sql);

      List<Field> headers = new ArrayList<>();
      ResultSetMetaData metadata = tableResult.getMetaData();
      // get the column names; column indexes start from 1
      for (int i = 1; i < metadata.getColumnCount() + 1; i++) {
        headers.add(new Field(metadata.getColumnName(i), SnowflakeUtil.sqlTypeToClass(metadata.getColumnType(i))));
      }
      List<List<Object>> values = new ArrayList<>();
      headers.forEach(field -> values.add(new ArrayList<>()));
      while (tableResult.next()) {
        for (int i = 0; i < headers.size(); i++) {
          values.get(i).add(getTypeValue(tableResult, i));
        }
      }

      return new ColumnarTable(
              headers,
              query.measures,
              IntStream.range(query.select.size(), query.select.size() + query.measures.size()).toArray(),
              IntStream.range(0, query.select.size()).toArray(),
              values);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the value with the correct type, otherwise everything is read as Object.
   */
  public static Object getTypeValue(ResultSet tableResult, int index) throws SQLException {
    return switch (tableResult.getMetaData().getColumnType(1 + index)) {
      case Types.CHAR, Types.NVARCHAR, Types.VARCHAR, Types.LONGVARCHAR -> tableResult.getString(1 + index);
      case Types.BOOLEAN, Types.BIT -> tableResult.getBoolean(1 + index);
      case Types.TINYINT -> tableResult.getByte(1 + index);
      case Types.SMALLINT -> tableResult.getShort(1 + index);
      case Types.INTEGER -> tableResult.getInt(1 + index);
      case Types.BIGINT -> tableResult.getLong(1 + index);
      case Types.REAL, Types.FLOAT -> tableResult.getFloat(1 + index);
      case Types.DECIMAL, Types.DOUBLE -> tableResult.getDouble(1 + index);
      case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> tableResult.getBytes(1 + index);
      case Types.DATE -> tableResult.getDate(1 + index);
      case Types.TIME -> tableResult.getTime(1 + index);
      case Types.TIMESTAMP -> tableResult.getTimestamp(1 + index);
      default -> tableResult.getObject(1 + index);
    };
  }

  @Override
  public List<String> supportedAggregationFunctions() {
    return SUPPORTED_AGGREGATION_FUNCTIONS;
  }

  static class SnowflakeQueryRewriter implements QueryRewriter {

    @Override
    public String tableName(String table) {
      return SqlUtils.doubleQuoteEscape(table);
    }

    @Override
    public String fieldName(String field) {
      return SqlUtils.doubleQuoteEscape(field);
    }

    @Override
    public String measureAlias(String alias) {
      return SqlUtils.doubleQuoteEscape(alias);
    }

    @Override
    public String rollup(String rollup) {
      return SqlUtils.doubleQuoteEscape(rollup);
    }

    @Override
    public String groupingAlias(String field) {
      return SqlUtils.doubleQuoteEscape(QueryRewriter.super.groupingAlias(field));
    }

    @Override
    public boolean doesSupportPartialRollup() {
      return true;
    }
  }
}
