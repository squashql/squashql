package me.paulbares.query.database;

import com.google.cloud.bigquery.*;
import me.paulbares.BigQueryDatastore;
import me.paulbares.BigQueryUtil;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.ColumnarTable;
import me.paulbares.query.QueryExecutor;
import me.paulbares.query.Table;
import me.paulbares.store.Field;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

public class BigQueryEngine extends AQueryEngine<BigQueryDatastore> {

  public static final String NULL_VALUE = "___null___";

  @Override
  protected String createSqlStatement(DatabaseQuery query) {
    boolean hasRollup = !query.rollup.isEmpty();
    if (!hasRollup) {
      return super.createSqlStatement(query);
    } else {
      BigQueryQueryRewriter rewriter = (BigQueryQueryRewriter) this.queryRewriter;
      BigQueryQueryRewriter newRewriter = new BigQueryQueryRewriter(rewriter.projectId, rewriter.datasetName) {
        @Override
        public String select(String select) {
          return String.format("coalesce(%s, \"%s\")", rewriter.select(select), NULL_VALUE);
        }

        @Override
        public String rollup(String rollup) {
          return String.format("coalesce(%s, \"%s\")", rewriter.rollup(rollup), NULL_VALUE);
        }
      };
      List<String> missingColumnsInRollup = new ArrayList<>(query.select);
      missingColumnsInRollup.removeAll(query.rollup);
      DatabaseQuery deepCopy = JacksonUtil.deserialize(JacksonUtil.serialize(query), DatabaseQuery.class);
      // Missing columns needs to be added at the beginning to have the correct sub-totals
      missingColumnsInRollup.addAll(query.rollup);
      deepCopy.rollup = missingColumnsInRollup;
      String translate = SQLTranslator.translate(deepCopy, QueryExecutor.withFallback(this.fieldSupplier, String.class), newRewriter);
      return translate;
    }
  }

  @Override
  protected Table postProcessDataset(Table input, DatabaseQuery query) {
    if (!query.rollup.isEmpty()) {
      boolean isPartialRollup = !Set.copyOf(query.select).equals(Set.copyOf(query.rollup));
      List<String> missingColumnsInRollup = new ArrayList<>(query.select);
      missingColumnsInRollup.removeAll(query.rollup);

      MutableIntSet rowIndicesToRemove = new IntHashSet();
      for (int i = 0; i < input.headers().size(); i++) {
        Field header = input.headers().get(i);
        List<Object> columnValues = input.getColumn(i);
        if (i < query.select.size()) {
          List<Object> baseColumnValues = input.getColumnValues(header.name());
          for (int rowIndex = 0; rowIndex < input.count(); rowIndex++) {
            Object value = columnValues.get(rowIndex);
            if (value == null) {
              baseColumnValues.set(rowIndex, SQLTranslator.TOTAL_CELL);
              if (isPartialRollup && missingColumnsInRollup.contains(header.name())) {
                // Partial rollup not supported https://issuetracker.google.com/issues/35905909, we let bigquery compute
                // all totals, and we remove here the extra rows.
                rowIndicesToRemove.add(rowIndex);
              }
            } else if (value.equals(NULL_VALUE)) {
              baseColumnValues.set(rowIndex, null);
            }
          }
        }
      }

      List<List<Object>> newValues;
      if (!rowIndicesToRemove.isEmpty()) {
        newValues = new ArrayList<>(input.headers().size());
        for (int col = 0; col < input.headers().size(); col++) {
          List<Object> columnValues = input.getColumn(col);
          List<Object> newColumnValues = new ArrayList<>(columnValues.size() - rowIndicesToRemove.size());
          for (int rowIndex = 0; rowIndex < input.count(); rowIndex++) {
            if (!rowIndicesToRemove.contains(rowIndex)) {
              newColumnValues.add(columnValues.get(rowIndex));
            }
          }
          newValues.add(newColumnValues);
        }
      } else {
        newValues = ((ColumnarTable) input).getColumns();
      }

      return new ColumnarTable(
              input.headers(),
              input.measures(),
              input.measureIndices(),
              input.columnIndices(),
              newValues);
    } else {
      return input;
    }
  }

  public BigQueryEngine(BigQueryDatastore datastore) {
    super(datastore, new BigQueryQueryRewriter(datastore.projectId, datastore.datasetName));
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query, String sql) {
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
              this.queryRewriter
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

  static class BigQueryQueryRewriter implements QueryRewriter {

    private final String projectId;
    private final String datasetName;

    BigQueryQueryRewriter(String projectId, String datasetName) {
      this.projectId = projectId;
      this.datasetName = datasetName;
    }

    @Override
    public String tableName(String table) {
      return SqlUtils.escape(this.projectId + "." + this.datasetName + "." + table);
    }

    @Override
    public String select(String select) {
      return SqlUtils.escape(select);
    }

    @Override
    public String rollup(String rollup) {
      return SqlUtils.escape(rollup);
    }

    /**
     * See <a href="https://cloud.google.com/bigquery/docs/schemas#column_names">https://cloud.google.com/bigquery/docs/schemas#column_names</a>.
     * A column name must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_), and it must start with a
     * letter or underscore. The maximum column name length is 300 characters.
     * FIXME must used a regex instead to replace incorrect characters.
     */
    @Override
    public String measureAlias(String alias) {
      return alias
              .replace("(", "_")
              .replace(")", "_")
              .replace(" ", "_");
    }

    @Override
    public boolean doesSupportPartialRollup() {
      // Not supported https://issuetracker.google.com/issues/35905909
      return false;
    }

    @Override
    public boolean doesSupportGroupingFunction() {
      // Not supported https://issuetracker.google.com/issues/205238172
      return false;
    }
  }
}
