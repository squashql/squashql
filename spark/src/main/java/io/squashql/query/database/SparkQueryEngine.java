package io.squashql.query.database;

import io.squashql.SparkDatastore;
import io.squashql.query.ColumnarTable;
import io.squashql.query.Header;
import io.squashql.query.RowTable;
import io.squashql.query.Table;
import io.squashql.store.Field;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.eclipse.collections.api.tuple.Pair;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static io.squashql.SparkUtil.datatypeToClass;

public class SparkQueryEngine extends AQueryEngine<SparkDatastore> {

  /**
   * https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html
   */
  public static final List<String> SUPPORTED_AGGREGATION_FUNCTIONS = List.of(
          "any",
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
          "variance");

  public SparkQueryEngine(SparkDatastore datastore) {
    super(datastore, DefaultQueryRewriter.INSTANCE);
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query, String sql) {
    Dataset<Row> ds = this.datastore.spark.sql(sql);
    Pair<List<Header>, List<List<Object>>> result = transformToColumnFormat(
            query,
            Arrays.stream(ds.schema().fields()).toList(),
            (column, name) -> new Field(null, name, datatypeToClass(column.dataType())),
            ds.toLocalIterator(),
            (i, r) -> r.get(i),
            this.queryRewriter);
    return new ColumnarTable(
            result.getOne(),
            new HashSet<>(query.measures),
            result.getTwo());
  }

  @Override
  public Table executeRawSql(String sql) {
    Dataset<Row> ds = this.datastore.spark.sql(sql);
    Pair<List<Header>, List<List<Object>>> result = transformToRowFormat(
            Arrays.stream(ds.schema().fields()).toList(),
            c -> new Field(null, c.name(), datatypeToClass(c.dataType())),
            ds.toLocalIterator(),
            (i, r) -> r.get(i));
    return new RowTable(result.getOne(), result.getTwo());
  }

  @Override
  public List<String> supportedAggregationFunctions() {
    return SUPPORTED_AGGREGATION_FUNCTIONS;
  }
}
