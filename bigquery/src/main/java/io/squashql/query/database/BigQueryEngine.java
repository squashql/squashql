package io.squashql.query.database;

import com.google.cloud.bigquery.*;
import io.squashql.BigQueryDatastore;
import io.squashql.BigQueryUtil;
import io.squashql.jackson.JacksonUtil;
import io.squashql.query.Header;
import io.squashql.query.QueryExecutor;
import io.squashql.table.ColumnarTable;
import io.squashql.table.RowTable;
import io.squashql.table.Table;
import io.squashql.type.TypedField;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.squashql.query.database.SQLTranslator.checkRollupIsValid;

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
  protected String createSqlStatement(DatabaseQuery query, QueryExecutor.PivotTableContext context) {
    boolean hasRollup = !query.rollup.isEmpty();
    BigQueryQueryRewriter rewriter = (BigQueryQueryRewriter) this.queryRewriter;
    Function<String, TypedField> queryFieldSupplier = QueryExecutor.createQueryFieldSupplier(this, query.virtualTableDto);
    if (!query.groupingSets.isEmpty()) {
      // rows = a,b,c; columns = x,y
      // (a,b,c,x,y)
      // (a,b,x,y,c)
      // (a,x,y,b,c)
      // (x,y,a,b,c)
      List<TypedField> l = new ArrayList<>(context.getRowFields());
      l.addAll(context.getColumnFields());
      List<List<TypedField>> rollups = new ArrayList<>();
      rollups.add(l);
      for (int i = 0; i < context.getRowFields().size(); i++) {
        List<TypedField> copy = new ArrayList<>(context.getRowFields());
        copy.addAll(i, context.getColumnFields());
        rollups.add(copy);
      }

      StringBuilder sb = new StringBuilder();
      String unionDistinct = " union distinct ";

      for (int i = 0; i < rollups.size(); i++) {
        DatabaseQuery deepCopy = JacksonUtil.deserialize(JacksonUtil.serialize(query), DatabaseQuery.class);
        deepCopy.groupingSets = Collections.emptyList();
        deepCopy.rollup = rollups.get(i);

        boolean isNotLast = i < rollups.size() - 1;
        if (isNotLast) {
          deepCopy.limit = -1; // only for the last one.
        }
        sb.append(createSqlStatement(deepCopy, null));
        if (isNotLast) {
          sb.append(unionDistinct);
        }
      }
      return sb.toString();
    }

    if (!hasRollup) {
      return SQLTranslator.translate(query,
              queryFieldSupplier,
              rewriter);
    } else {
      checkRollupIsValid(
              query.select.stream().map(f -> this.queryRewriter.select(f)).toList(),
              query.rollup.stream().map(f -> this.queryRewriter.rollup(f)).toList());

      // Special case for BigQuery because it does not support either the grouping function used to identify extra-rows added
      // by rollup or grouping sets for partial rollup.

      /*
       * The trick here is to change what is put in select and rollup:
       * SELECT SUM(amount) as amount_sum, COALESCE(a, "r1"), COALESCE(b, "r2") FROM table
       * GROUP BY ROLLUP(COALESCE(a, "___null___"), COALESCE(b, "___null___"))
       * By doing so, null values (not the ones due to rollup) will be changed to "___null___" and null values in the
       * result dataset correspond to the subtotals.
       */
      QueryAwareQueryRewriter qr = new QueryAwareQueryRewriter(rewriter, query);
      BigQueryQueryRewriter newRewriter = new BigQueryQueryRewriter(rewriter.projectId, rewriter.datasetName) {

        @Override
        public String select(TypedField field) {
          Function<Object, String> quoter = SQLTranslator.getQuoteFn(field);
          return String.format("coalesce(%s, %s)", qr.select(field),
                  quoter.apply(BigQueryUtil.getNullValue(field.type())));
        }

        @Override
        public String rollup(TypedField field) {
          Function<Object, String> quoter = SQLTranslator.getQuoteFn(field);
          return String.format("coalesce(%s, %s)", qr.rollup(field),
                  quoter.apply(BigQueryUtil.getNullValue(field.type())));
        }

        @Override
        public String getFieldFullName(TypedField f) {
          return qr.getFieldFullName(f);
        }
      };

      List<TypedField> missingColumnsInRollup = new ArrayList<>(query.select);
      missingColumnsInRollup.removeAll(query.rollup);
      DatabaseQuery deepCopy = JacksonUtil.deserialize(JacksonUtil.serialize(query), DatabaseQuery.class);
      // Missing columns needs to be added at the beginning to have the correct sub-totals
      missingColumnsInRollup.addAll(query.rollup);
      deepCopy.rollup = missingColumnsInRollup;
      return SQLTranslator.translate(deepCopy,
              queryFieldSupplier,
              // The condition on rollup is to handle subquery
              q -> q.rollup.isEmpty() ? this.queryRewriter : newRewriter);
    }
  }

  @Override
  protected Table postProcessDataset(Table input, DatabaseQuery query) {
    if (query.rollup.isEmpty() && query.groupingSets.isEmpty()) {
      return input;
    }

    boolean isPartialRollup = !Set.copyOf(query.select).equals(Set.copyOf(query.rollup));
    List<TypedField> missingColumnsInRollup = new ArrayList<>(query.select);
    missingColumnsInRollup.removeAll(query.rollup);
    Set<String> missingColumnsInRollupSet = missingColumnsInRollup.stream().map(SqlUtils::getFieldFullName).collect(Collectors.toSet());

    MutableIntSet rowIndicesToRemove = new IntHashSet();
    for (int i = 0; i < input.headers().size(); i++) {
      Header h = input.headers().get(i);
      List<Object> columnValues = input.getColumn(i);
      if (i < query.select.size()) {
        List<Object> baseColumnValues = input.getColumnValues(h.name());
        for (int rowIndex = 0; rowIndex < input.count(); rowIndex++) {
          Object value = columnValues.get(rowIndex);
          if (value == null) {
            baseColumnValues.set(rowIndex, SQLTranslator.TOTAL_CELL);
            if (query.groupingSets.isEmpty() && isPartialRollup && missingColumnsInRollupSet.contains(h.name())) {
              // Partial rollup not supported https://issuetracker.google.com/issues/35905909, we let bigquery compute
              // all totals, and we remove here the extra rows.
              rowIndicesToRemove.add(rowIndex);
            }
          } else if (value.equals(BigQueryUtil.getNullValue(h.type()))) {
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
            newValues);
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

    @Override
    public boolean useGroupingFunction() {
      // Not supported https://issuetracker.google.com/issues/205238172
      return false;
    }
  }
}
