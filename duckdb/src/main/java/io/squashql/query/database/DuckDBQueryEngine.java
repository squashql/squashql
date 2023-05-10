package io.squashql.query.database;

import io.squashql.DuckDBDatastore;
import io.squashql.DuckDBUtil;
import io.squashql.query.ColumnarTable;
import io.squashql.query.Header;
import io.squashql.query.Table;
import org.duckdb.DuckDBColumnType;
import org.eclipse.collections.api.tuple.Pair;

import java.io.Serializable;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class DuckDBQueryEngine extends AQueryEngine<DuckDBDatastore> {

  public DuckDBQueryEngine(DuckDBDatastore datastore) {
    super(datastore, new DuckDBQueryRewriter());
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query, String sql) {
    return executeQuery(sql, tableResult -> {
      List<Class<?>> columnTypes = new ArrayList<>();
      for (int i = 0; i < tableResult.getMetaData().getColumnCount(); i++) {
        Class<?> klass = DuckDBUtil.sqlTypeToClass(tableResult.getMetaData().getColumnType(i + 1));
        // Special case for HUGEINT. See also #getTypeValue
        if (klass == Object.class) {
          String columnTypeName = tableResult.getMetaData().getColumnTypeName(i + 1);
          if (columnTypeName.equals(DuckDBColumnType.HUGEINT.name())) {
            klass = long.class;
          }
        }
        columnTypes.add(klass);
      }

      Pair<List<Header>, List<List<Object>>> result = transformToColumnFormat(
              query,
              columnTypes,
              (columnType, name) -> name,
              (columnType, name) -> columnType,
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
    private boolean isLast = false;

    private ResultSetIterator(ResultSet resultSet) throws SQLException {
      this.resultSet = resultSet;
//      this.isEmpty = !this.resultSet.next(); // the only way to know if it is empty, but we will have to ignore next() once to not miss the first row.
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

  protected <R> R executeQuery(String sql, ThrowingFunction<ResultSet, R> consumer) {
    try (Statement snowflakeStatement = this.datastore.getConnection().createStatement()) {
      {
        ResultSet tableResult = snowflakeStatement.executeQuery("select \"scenario\" from mystore");
        System.out.println();
      }
      {
        ResultSet tableResult = snowflakeStatement.executeQuery(sql);
        return consumer.apply(tableResult);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @FunctionalInterface
  public interface ThrowingFunction<T, R> extends Serializable {

    @SuppressWarnings("ProhibitedExceptionDeclared")
    R apply(T t) throws SQLException;
  }

  @Override
  public Table executeRawSql(String sql) {
    return null;
  }

  @Override
  public List<String> supportedAggregationFunctions() {
    return null;
  }

  static class DuckDBQueryRewriter implements QueryRewriter {

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
