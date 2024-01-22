package io.squashql.query.database;

import io.squashql.DuckDBDatastore;
import io.squashql.DuckDBUtil;
import io.squashql.jdbc.JdbcQueryEngine;

import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.function.BiFunction;

public class DuckDBQueryEngine extends JdbcQueryEngine<DuckDBDatastore> {

  /**
   * https://duckdb.org/docs/sql/aggregates. NOTE there is more but only a subset is proposed here.
   */
  public static final List<String> SUPPORTED_AGGREGATION_FUNCTIONS = List.of(
          "ANY_VALUE",
          "AVG",
          "CORR",
          "COUNT",
          "COVAR_POP",
          "MAX",
          "MEDIAN",
          "MIN",
          "MODE",
          "STDDEV_POP",
          "STDDEV_SAMP",
          "SUM",
          "VAR_POP",
          "VAR_SAMP");

  public DuckDBQueryEngine(DuckDBDatastore datastore) {
    super(datastore);
  }

  @Override
  protected BiFunction<ResultSetMetaData, Integer, Class<?>> typeToClassConverter() {
    return DuckDBUtil::sqlTypeToClass;
  }

  @Override
  public List<String> supportedAggregationFunctions() {
    return SUPPORTED_AGGREGATION_FUNCTIONS;
  }

  @Override
  public QueryRewriter queryRewriter(DatabaseQuery query) {
    return new DuckDBQueryRewriter(query){
//      @Override
//      public String tableName(String table) {
//        return super.tableName(table + "_table_");
//      }

      @Override
      public String cteName(String cteName) {
        return super.cteName(cteName + "_cte_");
      }
    };
  }
}
