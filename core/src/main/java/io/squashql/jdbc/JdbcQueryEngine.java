package io.squashql.jdbc;

import io.squashql.query.Header;
import io.squashql.query.database.AQueryEngine;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.table.ColumnarTable;
import io.squashql.table.RowTable;
import io.squashql.table.Table;
import org.eclipse.collections.api.tuple.Pair;

import java.io.Serializable;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

public abstract class JdbcQueryEngine<T extends JdbcDatastore> extends AQueryEngine<T> {

  public JdbcQueryEngine(T datastore) {
    super(datastore);
  }

  protected abstract BiFunction<ResultSetMetaData, Integer, Class<?>> typeToClassConverter();

  @Override
  protected Table retrieveAggregates(DatabaseQuery query, String sql) {
    return executeQuery(sql, this.datastore.getConnection(), tableResult -> {
      List<Class<?>> columnTypes = getColumnTypes(tableResult.getMetaData());
      ResultSetReader reader = createResultSetReader();
      Pair<List<Header>, List<List<Object>>> result = transformToColumnFormat(
              query.select,
              query.measures,
              columnTypes,
              (columnType, name) -> columnType,
              new ResultSetIterator(columnTypes, tableResult, reader),
              (i, fieldValues) -> fieldValues[i]);
      return new ColumnarTable(
              result.getOne(),
              new HashSet<>(query.measures),
              result.getTwo());
    });
  }

  @Override
  public Table executeRawSql(String sql) {
    return executeQuery(sql, this.datastore.getConnection(), tableResult -> {
      List<Header> headers = createHeaderList(tableResult, Collections.emptySet());
      List<Class<?>> columnTypes = getColumnTypes(tableResult.getMetaData());
      ResultSetReader reader = createResultSetReader();
      List<List<Object>> rows = new ArrayList<>();
      while (tableResult.next()) {
        rows.add(IntStream.range(0, headers.size()).mapToObj(i -> reader.read(columnTypes, tableResult, i)).toList());
      }
      return new RowTable(headers, rows);
    });
  }

  public boolean executeSql(String sql) {
    return execute(sql, this.datastore.getConnection());
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

  protected ResultSetReader createResultSetReader() {
    return new ResultSetReader() {
    };
  }

  protected List<Class<?>> getColumnTypes(ResultSetMetaData metaData) throws SQLException {
    List<Class<?>> columnTypes = new ArrayList<>();
    for (int i = 0; i < metaData.getColumnCount(); i++) {
      columnTypes.add(typeToClassConverter().apply(metaData, i + 1));
    }
    return columnTypes;
  }

  private static class ResultSetIterator implements Iterator<Object[]> {

    private final List<Class<?>> columnTypes;
    private final ResultSet resultSet;
    private final ResultSetReader reader;
    private final Object[] buffer;

    private ResultSetIterator(List<Class<?>> columnTypes, ResultSet resultSet, ResultSetReader reader) {
      this.columnTypes = columnTypes;
      this.resultSet = resultSet;
      this.buffer = new Object[columnTypes.size()];
      this.reader = reader;
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
        this.buffer[i] = this.reader.read(this.columnTypes, this.resultSet, i);
      }
      return this.buffer;
    }
  }

  /**
   * Gets the value with the correct type, otherwise everything is read as Object.
   */
  public static Object getTypeValue(List<Class<?>> columnTypes, ResultSet tableResult, int index) {
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
        case Types.DATE -> {
          Date d = tableResult.getDate(1 + index);
          yield d == null ? null : d.toLocalDate();
        }
        case Types.TIME -> tableResult.getTime(1 + index);
        case Types.TIMESTAMP -> tableResult.getTimestamp(1 + index);
        default -> {
          Object object = tableResult.getObject(1 + index);
          if (object instanceof BigInteger) {
            yield ((BigInteger) object).longValueExact();
          } else if (object instanceof Array a) {
            yield JdbcUtil.sqlArrayToList(columnTypes.get(index), a);
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

  protected boolean execute(String sql, Connection connection) {
    try (Statement statement = connection.createStatement()) {
      return statement.execute(sql);
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
