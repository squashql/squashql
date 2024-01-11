package io.squashql.query.database;

import com.google.cloud.bigquery.*;
import io.squashql.BigQueryDatastore;
import io.squashql.BigQueryUtil;
import io.squashql.jdbc.JdbcUtil;
import io.squashql.list.Lists;
import io.squashql.query.Header;
import io.squashql.table.ColumnarTable;
import io.squashql.table.RowTable;
import io.squashql.table.Table;
import org.eclipse.collections.api.tuple.Pair;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;

public class BigQueryEngine extends AQueryEngine<BigQueryDatastore> {

  /**
   * https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#covar_samp
   * https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions
   */
  public static final List<String> SUPPORTED_AGGREGATION_FUNCTIONS = List.of(
          "any_value",
          "avg",
          "corr",
          "count",
          "covar_pop",
          "covar_samp",
          "min",
          "max",
          "stddev_pop",
          "stddev_samp",
          "sum",
          "var_pop",
          "var_samp",
          "variance"
  );

  public BigQueryEngine(BigQueryDatastore datastore) {
    super(datastore);
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query, String sql) {
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
    try {
      TableResult tableResult = this.datastore.getBigquery().query(queryConfig);
      Schema schema = tableResult.getSchema();
      Pair<List<Header>, List<List<Object>>> result = transformToColumnFormat(
              query.select,
              query.measures,
              schema.getFields(),
              (column, name) -> BigQueryUtil.bigQueryTypeToClass(column),
              tableResult.iterateAll().iterator(),
              (i, fieldValueList) -> getTypeValue(fieldValueList, schema, i));
      return new ColumnarTable(
              result.getOne(),
              new HashSet<>(query.measures),
              result.getTwo());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Table executeRawSql(String sql) {
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
    try {
      TableResult tableResult = this.datastore.getBigquery().query(queryConfig);
      Schema schema = tableResult.getSchema();
      Pair<List<Header>, List<List<Object>>> result = transformToRowFormat(
              schema.getFields(),
              column -> column.getName(),
              column -> BigQueryUtil.bigQueryTypeToClass(column),
              tableResult.iterateAll().iterator(),
              (i, fieldValueList) -> getTypeValue(fieldValueList, schema, i));
      return new RowTable(result.getOne(), result.getTwo());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the value with the correct type, otherwise everything is read as String.
   */
  public static Object getTypeValue(FieldValueList fieldValues, Schema schema, int index) {
    FieldValue fieldValue = fieldValues.get(index);
    if (fieldValue.isNull()) {
      // There is a check in BQ client when trying to access the value and throw if null.
      return null;
    }
    com.google.cloud.bigquery.Field field = schema.getFields().get(index);
    if (field.getMode() == Field.Mode.REPEATED) {
      // It is an array
      Class<?> listClass = BigQueryUtil.bigQueryTypeToClass(field);
      return JdbcUtil.streamToList(listClass, fieldValue.getRepeatedValue().stream().map(o -> {
        if (listClass.equals(Lists.DoubleList.class)) {
          return o.getDoubleValue();
        } else if (listClass.equals(Lists.LongList.class)) {
          return o.getLongValue();
        } else if (listClass.equals(LocalDate.class)) {
          return LocalDate.parse(o.getStringValue());
        } else {
          return o.getValue(); // it will be a string
        }
      }));
    } else {
      return switch (field.getType().getStandardType()) {
        case BOOL -> fieldValue.getBooleanValue();
        case INT64 -> fieldValue.getLongValue();
        case FLOAT64 -> fieldValue.getDoubleValue();
        case BYTES -> fieldValue.getBytesValue();
        case DATE -> LocalDate.parse(fieldValue.getStringValue());
        default -> fieldValue.getValue();
      };
    }
  }

  @Override
  public List<String> supportedAggregationFunctions() {
    return SUPPORTED_AGGREGATION_FUNCTIONS;
  }

  @Override
  public QueryRewriter queryRewriter(DatabaseQuery query) {
    return new BigQueryQueryRewriter(this.datastore.getProjectId(), this.datastore.getDatasetName(), query);
  }
}
