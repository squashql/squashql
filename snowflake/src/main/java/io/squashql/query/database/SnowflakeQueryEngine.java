package io.squashql.query.database;

import io.squashql.SnowflakeDatastore;
import io.squashql.SnowflakeUtil;
import io.squashql.query.ColumnarTable;
import io.squashql.query.Header;
import io.squashql.query.RowTable;
import io.squashql.query.Table;
import io.squashql.store.Field;
import org.eclipse.collections.api.tuple.Pair;

import java.io.Serializable;
import java.sql.*;
import java.util.*;
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
      List<Class<?>> columnTypes = new ArrayList<>();
      for (int i = 0; i < tableResult.getMetaData().getColumnCount(); i++) {
        columnTypes.add(SnowflakeUtil.sqlTypeToClass(tableResult.getMetaData().getColumnType(i + 1)));
      }

      Pair<List<Header>, List<List<Object>>> result = transformToColumnFormat(
              query,
              columnTypes,
              (columnType, name) -> new Field(null, name, columnType),
              new ResultSetIterator(tableResult),
              (i, fieldValues) -> fieldValues[i],
              this.queryRewriter
      );
      return new ColumnarTable(
              result.getOne(),
              new HashSet<>(query.measures),
              result.getTwo());
    });
  }

  private static class ResultSetIterator implements Iterator<Object[]> {

    private final ResultSet resultSet;
    private final Object[] buffer;
    private boolean isEmpty;
    private boolean isFirst = true;

    private ResultSetIterator(ResultSet resultSet) throws SQLException {
      this.resultSet = resultSet;
      this.isEmpty = !this.resultSet.next(); // the only way to know if it is empty, but we will have to ignore next() once to not miss the first row.
      this.buffer = new Object[this.resultSet.getMetaData().getColumnCount()];
    }

    @Override
    public boolean hasNext() {
      if (this.isEmpty) {
        return false;
      }

      try {
        return this.isFirst || !this.resultSet.isLast();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Object[] next() {
      try {
        if (!this.isFirst && !this.resultSet.next()) { // do not call next the first time.
          throw new NoSuchElementException();
        }
        this.isFirst = false;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      for (int i = 0; i < this.buffer.length; i++) {
        this.buffer[i] = getTypeValue(this.resultSet, i);
      }
      return this.buffer;
    }
  }

  @Override
  public Table executeRawSql(String sql) {
    return executeQuery(sql, tableResult -> {
      List<Header> headers = createHeaderList(tableResult, Collections.emptySet());
      List<List<Object>> rows = new ArrayList<>();
      while (tableResult.next()) {
        rows.add(IntStream.range(0, headers.size()).mapToObj(i -> getTypeValue(tableResult, i)).toList());
      }
      return new RowTable(headers, rows);
    });
  }

  protected List<Header> createHeaderList(ResultSet tableResult, Set<String> measureNames) throws SQLException {
    List<Header> headers = new ArrayList<>();
    ResultSetMetaData metadata = tableResult.getMetaData();
    // get the column names; column indexes start from 1
    for (int i = 1; i < metadata.getColumnCount() + 1; i++) {
      String fieldName = metadata.getColumnName(i);
      headers.add(new Header(
              new Field(null, fieldName, SnowflakeUtil.sqlTypeToClass(metadata.getColumnType(i))),
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
    public String cteName(String cteName) {
      return SqlUtils.doubleQuoteEscape(cteName);
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
  }
}
