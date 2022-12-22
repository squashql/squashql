package me.paulbares.query.database;

import com.google.cloud.bigquery.*;
import me.paulbares.BigQueryDatastore;
import me.paulbares.BigQueryUtil;
import me.paulbares.query.ColumnarTable;
import me.paulbares.query.Table;
import me.paulbares.store.Field;
import org.eclipse.collections.api.tuple.Pair;

import java.util.List;
import java.util.stream.IntStream;

public class BigQueryEngine extends AQueryEngine<BigQueryDatastore> {

  private final QueryRewriter queryRewriter;

  public BigQueryEngine(BigQueryDatastore datastore) {
    super(datastore);
    this.queryRewriter = new BigQueryQueryRewriter();
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query) {
    String sql = SQLTranslator.translate(query, this.fieldSupplier, this.queryRewriter, (qr, name) -> qr.tableName(name));
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
    try {
      TableResult tableResult = this.datastore.getBigquery().query(queryConfig);
      Schema schema = tableResult.getSchema();
      Pair<List<Field>, List<List<Object>>> result = AQueryEngine.transform(
              query,
              schema.getFields(),
              (column, name) -> new Field(name, BigQueryUtil.bigQueryTypeToClass(column.getType())),
              tableResult.iterateAll().iterator(),
              (i, fieldValueList) -> getTypeValue(fieldValueList, schema, i),
              queryRewriter
      );
      return new ColumnarTable(
              result.getOne(),
              query.measures,
              IntStream.range(query.select.size(), query.select.size() + query.measures.size()).toArray(),
              IntStream.range(0, query.select.size()).toArray(),
              result.getTwo());
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
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/statistical_aggregate_functions#covar_samp
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions
    return List.of(
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
  }

  class BigQueryQueryRewriter implements QueryRewriter {
    @Override
    public String fieldName(String field) {
      return SqlUtils.backtickEscape(field);
    }

    @Override
    public String tableName(String table) {
      return SqlUtils.backtickEscape(BigQueryEngine.this.datastore.projectId + "." + BigQueryEngine.this.datastore.datasetName + "." + table);
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
    public String rollup(String rollup) {
      return SqlUtils.backtickEscape(rollup);
    }

    @Override
    public String groupingAlias(String field) {
      return SqlUtils.backtickEscape(QueryRewriter.super.groupingAlias(field));
    }

    @Override
    public boolean doesSupportPartialRollup() {
      return true;
    }
  }
}
