package io.squashql.query.database;

import io.squashql.PostgreSQLDatastore;
import io.squashql.PostgreSQLUtil;
import io.squashql.jdbc.JdbcQueryEngine;

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
  protected BiFunction<Integer, Object[], Object> recordToFieldValue() {
    return (i, values) -> PostgreSQLUtil.getTypeValue(values[i]);
  }

  @Override
  protected BiFunction<ResultSetMetaData, Integer, Class<?>> typeToClassConverter() {
    return (metaData, column) -> {
      try {
        String columnTypeName = metaData.getColumnTypeName(column);
        int dataColumnType = metaData.getColumnType(column);
        return PostgreSQLUtil.sqlTypeToJavaClass(dataColumnType, columnTypeName);
      } catch (SQLException e) {
        throw new RuntimeException(e);
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
