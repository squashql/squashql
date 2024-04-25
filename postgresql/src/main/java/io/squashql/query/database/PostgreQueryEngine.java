package io.squashql.query.database;

import io.squashql.PostgreDatastore;
import io.squashql.PostgreUtil;
import io.squashql.jdbc.JdbcQueryEngine;
import io.squashql.jdbc.JdbcUtil;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiFunction;

public class PostgreQueryEngine extends JdbcQueryEngine<PostgreDatastore> {

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

  public PostgreQueryEngine(PostgreDatastore datastore) {
    super(datastore);
  }

  @Override
  protected BiFunction<Integer, Object[], Object> recordToFieldValue() {
    return (i, values) -> PostgreUtil.getTypeValue(values[i]);
  }

  @Override
  protected BiFunction<ResultSetMetaData, Integer, Class<?>> typeToClassConverter() {
    return (metaData, column) -> {
      try {
//        String columnTypeName = metaData.getColumnTypeName(column);
//        if (columnTypeName.equals("ARRAY")) {
//          return List.class;
//        } else {
        return JdbcUtil.sqlTypeToClass(metaData.getColumnType(column));
//        }
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
    return new PostgreQueryRewriter();
  }
//
//  static class ClickHouseQueryRewriter implements QueryRewriter {
//
//    @Override
//    public String fieldName(String field) {
//      return SqlUtils.backtickEscape(field);
//    }
//
//    @Override
//    public String escapeAlias(String alias) {
//      return SqlUtils.backtickEscape(alias);
//    }
//
//    @Override
//    public boolean usePartialRollupSyntax() {
//      // Not supported as of now: https://github.com/ClickHouse/ClickHouse/issues/322#issuecomment-615087004
//      // Tested with version https://github.com/ClickHouse/ClickHouse/tree/v22.10.2.11-stable
//      return false;
//    }
//
//    @Override
//    public String arrayContains(TypedField field, Object value) {
//      return "has(" + field.sqlExpression(this) + ", " + value + ")";
//    }
//  }
}
