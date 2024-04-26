package io.squashql.query.database;

import io.squashql.PostgreSQLDatastore;
import io.squashql.PostgreSQLUtil;
import io.squashql.jdbc.JdbcQueryEngine;
import io.squashql.jdbc.ResultSetReader;
import io.squashql.util.Types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiFunction;

public class PostgreSQLQueryEngine extends JdbcQueryEngine<PostgreSQLDatastore> {

  /**
   * FIXME
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
        // Special case for Snowflake due to lack of support of Array
        if (columnTypes.get(index).equals(BigDecimal.class)) {
          try {
            if (tableResult.getObject(index + 1) == null) {
              return null;
            }
            return Types.castToDouble(tableResult.getBigDecimal(index + 1));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        } else if (columnTypes.get(index).equals(BigInteger.class)) {
          try {
            if (tableResult.getObject(index + 1) == null) {
              return null;
            }
            return tableResult.getLong(index + 1);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        } else {
          return ResultSetReader.super.read(columnTypes, tableResult, index);
        }
      }
    };
  }

  @Override
  public List<String> supportedAggregationFunctions() {
    return SUPPORTED_AGGREGATION_FUNCTIONS;
  }

  @Override
  public QueryRewriter queryRewriter(DatabaseQuery query) {
    return new PostgreSQLQueryRewriter();
  }
}
