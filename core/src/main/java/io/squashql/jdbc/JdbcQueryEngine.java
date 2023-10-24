package io.squashql.jdbc;

import io.squashql.query.database.QueryResultFormat;
import io.squashql.table.ColumnarTable;
import io.squashql.query.Header;
import io.squashql.table.RowTable;
import io.squashql.table.Table;
import io.squashql.query.database.AQueryEngine;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.QueryRewriter;
import org.eclipse.collections.api.tuple.Pair;

import java.io.Serializable;
import java.math.BigInteger;
import java.sql.*;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

public abstract class JdbcQueryEngine<T extends JdbcDatastore> extends AQueryEngine<T> {

  public JdbcQueryEngine(T datastore, QueryRewriter queryRewriter) {
    super(datastore, queryRewriter);
  }

  protected abstract BiFunction<ResultSetMetaData, Integer, Class<?>> typeToClassConverter();

  @Override
  protected Table retrieveAggregates(QueryResultFormat format, String sql) {
    return executeQuery(sql, this.datastore.getConnection(), tableResult -> {
      List<Class<?>> columnTypes = new ArrayList<>();
      for (int i = 0; i < tableResult.getMetaData().getColumnCount(); i++) {
        columnTypes.add(typeToClassConverter().apply(tableResult.getMetaData(), i + 1));
      }

      Pair<List<Header>, List<List<Object>>> result = transformToColumnFormat(
              format,
              columnTypes,
              (columnType, name) -> columnType,
              new ResultSetIterator(tableResult),
              (i, fieldValues) -> fieldValues[i]);
      return new ColumnarTable(
              result.getOne(),
              new HashSet<>(format.measures()),
              result.getTwo());
    });
  }

  @Override
  public Table executeRawSql(String sql) {
    return executeQuery(sql, this.datastore.getConnection(), tableResult -> {
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
      headers.add(new Header(fieldName, typeToClassConverter().apply(metadata, i), measureNames.contains(fieldName)));
    }

    return headers;
  }

  private static class ResultSetIterator implements Iterator<Object[]> {

    private final ResultSet resultSet;
    private final Object[] buffer;

    private ResultSetIterator(ResultSet resultSet) throws SQLException {
      this.resultSet = resultSet;
      this.buffer = new Object[this.resultSet.getMetaData().getColumnCount()];
    }

    @Override
    public boolean hasNext() {
      try {
        return this.resultSet.next();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Object[] next() {
      for (int i = 0; i < this.buffer.length; i++) {
        this.buffer[i] = getTypeValue(this.resultSet, i);
      }
      return this.buffer;
    }
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
        default -> {
          Object object = tableResult.getObject(1 + index);
          if (object instanceof BigInteger) {
            yield ((BigInteger) object).longValueExact();
          } else {
            yield object;
          }
        }
      };
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected <R> R executeQuery(String sql, Connection connection, ThrowingFunction<ResultSet, R> consumer) {
    try (Statement statement = connection.createStatement()) {
      ResultSet tableResult = statement.executeQuery(sql);
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
}
