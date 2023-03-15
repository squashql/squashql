package io.squashql.query.database;

import io.squashql.SnowflakeDatastore;
import io.squashql.SnowflakeUtil;
import io.squashql.query.*;
import io.squashql.store.FieldWithStore;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;

public class SnowflakeQueryEngine extends AQueryEngine<SnowflakeDatastore> {

  /**
   * https://docs.snowflake.com/en/sql-reference/functions-aggregation.html. NOTE there is more but only a subset is
   * proposed here.
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

  public SnowflakeQueryEngine(SnowflakeDatastore datastore) {
    super(datastore, new SnowflakeQueryRewriter());
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query, String sql) {
    return executeQuery(sql, tableResult -> {
      List<Header> headers = createHeaderList(tableResult, query.measures);
      List<List<Object>> values = new ArrayList<>();
      headers.forEach(field -> values.add(new ArrayList<>()));
      while (tableResult.next()) {
        for (int i = 0; i < headers.size(); i++) {
          values.get(i).add(getTypeValue(tableResult, i));
        }
      }
      return new ColumnarTable(
              headers,
              new HashSet<>(query.measures),
              values);
    });
  }

  @Override
  public Table executeRawSql(String sql) {
    return executeQuery(sql, tableResult -> {
      List<Header> headers = createHeaderList(tableResult, Collections.emptyList());
      List<List<Object>> rows = new ArrayList<>();
      while (tableResult.next()) {
        rows.add(IntStream.range(0, headers.size()).mapToObj(i -> getTypeValue(tableResult, i)).toList());
      }
      return new RowTable(headers, rows);
    });
  }

  protected List<Header> createHeaderList(ResultSet tableResult, List<Measure> measureNames) throws SQLException {
    List<Header> headers = new ArrayList<>();
    ResultSetMetaData metadata = tableResult.getMetaData();
    // get the column names; column indexes start from 1
    for (int i = 1; i < metadata.getColumnCount() + 1; i++) {
      String fieldName = metadata.getColumnName(i);
      headers.add(new Header(
              new FieldWithStore(null, fieldName, SnowflakeUtil.sqlTypeToClass(metadata.getColumnType(i))),
              measureNames.contains(fieldName)));
    }

    return headers;
  }

  protected <R> R executeQuery(String sql, ThrowingFunction<ResultSet, R> consumer) {
    try (Statement snowflakeStatement = this.datastore.getConnection().createStatement()) {
      ResultSet tableResult = snowflakeStatement.executeQuery(sql);
      return consumer.apply(tableResult);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @FunctionalInterface
  public interface ThrowingFunction<T, R> extends Serializable {

    @SuppressWarnings("ProhibitedExceptionDeclared")
    R apply(T t) throws SQLException;
  }

  /**
   * Gets the value with the correct type, otherwise everything is read as Object.
   */
  public static Object getTypeValue(ResultSet tableResult, int index) {
    try {
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
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
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
    public boolean usePartialRollupSyntax() {
      return true;
    }

    @Override
    public boolean useGroupingFunction() {
      return true;
    }

    @Override
    public String groupingAlias(String field) {
      return SqlUtils.doubleQuoteEscape(QueryRewriter.super.groupingAlias(field));
    }
  }
}
