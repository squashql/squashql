package io.squashql.query.database;

import io.squashql.PostgreSQLDatastore;
import io.squashql.PostgreSQLUtil;
import io.squashql.jdbc.JdbcQueryEngine;
import io.squashql.jdbc.ResultSetReader;
import io.squashql.util.Types;
import org.postgresql.util.PGobject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiFunction;

public class PostgreSQLQueryEngine extends JdbcQueryEngine<PostgreSQLDatastore> {

  /**
   * <a href="https://www.postgresql.org/docs/16/functions-aggregate.html/">aggregate functions</a>
   * NOTE: there is more but only a subset is proposed here.
   */
  public static final List<String> SUPPORTED_AGGREGATION_FUNCTIONS = List.of(
          "count",
          "min",
          "max",
          "sum",
          "avg",
          "any");

  public PostgreSQLQueryEngine(PostgreSQLDatastore datastore) {
    super(datastore);
  }

  @Override
  protected BiFunction<ResultSetMetaData, Integer, Class<?>> typeToClassConverter() {
    return (metaData, column) -> {
      try {
        return PostgreSQLUtil.getJavaClass(metaData, column);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    };
  }

  @Override
  protected ResultSetReader createResultSetReader() {
    return new ResultSetReader() {
      @Override
      public Object read(List<Class<?>> columnTypes, ResultSet tableResult, int index) {
        try {
          Object object = tableResult.getObject(index + 1);
          if (object == null) {
            return null;
          }

          if (object instanceof PGobject pgo) {
            return pgo.getValue(); // send a string
          }

          Class<?> klazz = columnTypes.get(index);
          if (klazz.equals(BigDecimal.class)) {
            return Types.castToDouble(tableResult.getBigDecimal(index + 1));
          } else if (klazz.equals(BigInteger.class)) {
            return tableResult.getLong(index + 1);
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return ResultSetReader.super.read(columnTypes, tableResult, index);
      }
    };
  }

  @Override
  public List<String> supportedAggregationFunctions() {
    return SUPPORTED_AGGREGATION_FUNCTIONS;
  }

  @Override
  public QueryRewriter queryRewriter() {
    return new PostgreSQLQueryRewriter();
  }
}
