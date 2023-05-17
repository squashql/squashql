package io.squashql.query.database;

import io.squashql.SnowflakeDatastore;
import io.squashql.jdbc.JdbcQueryEngine;
import io.squashql.jdbc.JdbcUtil;

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
    super(datastore, new SnowflakeQueryRewriter());
  }

  @Override
  protected BiFunction<ResultSetMetaData, Integer, Class<?>> typeToClassConverter() {
    return (metaData, column) -> {
      try {
        return JdbcUtil.sqlTypeToClass(metaData.getColumnType(column));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    };
  }

  @Override
  public List<String> supportedAggregationFunctions() {
    return SUPPORTED_AGGREGATION_FUNCTIONS;
  }
}
