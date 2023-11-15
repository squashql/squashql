package io.squashql.query.database;

import com.google.cloud.bigquery.*;
import io.squashql.BigQueryDatastore;
import io.squashql.BigQueryUtil;
import io.squashql.query.Header;
import io.squashql.query.date.DateFunctions;
import io.squashql.table.ColumnarTable;
import io.squashql.table.RowTable;
import io.squashql.table.Table;
import io.squashql.type.FunctionTypedField;
import org.eclipse.collections.api.tuple.Pair;

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
    super(datastore, new BigQueryQueryRewriter(datastore.getProjectId(), datastore.getDatasetName()));
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query, String sql) {
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
    try {
      TableResult tableResult = this.datastore.getBigquery().query(queryConfig);
      Schema schema = tableResult.getSchema();
      Pair<List<Header>, List<List<Object>>> result = transformToColumnFormat(
              query,
              schema.getFields(),
              (column, name) -> name,
              (column, name) -> BigQueryUtil.bigQueryTypeToClass(column.getType()),
              tableResult.iterateAll().iterator(),
              (i, fieldValueList) -> getTypeValue(fieldValueList, schema, i),
              this.queryRewriter
      );
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
              column -> BigQueryUtil.bigQueryTypeToClass(column.getType()),
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
    return switch (field.getType().getStandardType()) {
      case BOOL -> fieldValue.getBooleanValue();
      case INT64 -> fieldValue.getLongValue();
      case FLOAT64 -> fieldValue.getDoubleValue();
      case BYTES -> fieldValue.getBytesValue();
      default -> fieldValue.getValue();
    };
  }

  @Override
  public List<String> supportedAggregationFunctions() {
    return SUPPORTED_AGGREGATION_FUNCTIONS;
  }

  static class BigQueryQueryRewriter implements QueryRewriter {

    private final String projectId;
    private final String datasetName;

    BigQueryQueryRewriter(String projectId, String datasetName) {
      this.projectId = projectId;
      this.datasetName = datasetName;
    }

    @Override
    public String functionExpression(FunctionTypedField ftf) {
      if (DateFunctions.SUPPORTED_DATE_FUNCTIONS.contains(ftf.function())) {
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract
        return String.format("EXTRACT(%s FROM %s)", ftf.function(), getFieldFullName(ftf.field()));
      } else {
        throw new IllegalArgumentException("Unsupported function " + ftf);
      }
    }

    @Override
    public String fieldName(String field) {
      return SqlUtils.backtickEscape(field);
    }

    @Override
    public String tableName(String table) {
      return SqlUtils.backtickEscape(this.projectId + "." + this.datasetName + "." + table);
    }

    /**
     * See <a href="https://cloud.google.com/bigquery/docs/schemas#column_names">https://cloud.google.com/bigquery/docs/schemas#column_names</a>.
     * A column name must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_), and it must start with a
     * letter or underscore. The maximum column name length is 300 characters.
     * FIXME must used a regex instead to replace incorrect characters.
     */
    @Override
    public String measureAlias(String alias) {
      return SqlUtils.backtickEscape(alias)
              .replace("(", "_")
              .replace(")", "_")
              .replace(" ", "_");
    }

    @Override
    public boolean usePartialRollupSyntax() {
      // Not supported https://issuetracker.google.com/issues/35905909
      return false;
    }
  }
}
