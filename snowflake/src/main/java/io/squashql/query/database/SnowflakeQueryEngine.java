package io.squashql.query.database;

import io.squashql.SnowflakeDatastore;
import io.squashql.jdbc.JdbcQueryEngine;
import io.squashql.jdbc.JdbcUtil;
import io.squashql.jdbc.ResultSetReader;
import io.squashql.list.Lists;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiFunction;

public class SnowflakeQueryEngine extends JdbcQueryEngine<SnowflakeDatastore> {

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
    super(datastore);
  }

  @Override
  protected BiFunction<ResultSetMetaData, Integer, Class<?>> typeToClassConverter() {
    return (metaData, column) -> {
      try {
        String columnTypeName = metaData.getColumnTypeName(column);
        if (columnTypeName.equals("ARRAY")) {
          // snowflake does not support array. It will return a string. For now, we simply consider it is an array of object
          // since we do not have access to the type of the elements... Hopefully it will be fixed in the future...
          return List.class;
        } else {
          return JdbcUtil.sqlTypeToClass(metaData.getColumnType(column));
        }
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
        if (columnTypes.get(index).equals(List.class)) {
          try {
            return parseVectorString(tableResult.getString(index + 1));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        } else {
          return ResultSetReader.super.read(columnTypes, tableResult, index);
        }
      }
    };
  }

  /**
   * Parses a string representation of a vector and returns a list of doubles.
   *
   * @param vectorString The string representation of the vector.
   * @return A list of doubles representing the vector.
   */
  public static Lists.DoubleList parseVectorString(String vectorString) {
    // It is a string that needs to be parsed, see https://github.com/snowflakedb/snowflake-jdbc/issues/462
    String[] split = vectorString.replace("\n", "")
            .replace("[", "")
            .replace("]", "")
            .split(",");
    Lists.DoubleList r = new Lists.DoubleList();
    for (String s : split) {
      BigDecimal bigDecimal = new BigDecimal(s.trim());
      r.add(bigDecimal.doubleValue());
    }
    return r;
  }

  @Override
  public List<String> supportedAggregationFunctions() {
    return SUPPORTED_AGGREGATION_FUNCTIONS;
  }

  @Override
  public QueryRewriter queryRewriter() {
    return new SnowflakeQueryRewriter();
  }
}
