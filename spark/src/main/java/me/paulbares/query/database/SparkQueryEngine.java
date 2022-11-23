package me.paulbares.query.database;

import me.paulbares.SparkDatastore;
import me.paulbares.query.ColumnarTable;
import me.paulbares.query.Table;
import me.paulbares.store.Field;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.eclipse.collections.api.tuple.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static me.paulbares.SparkUtil.datatypeToClass;

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
    super(datastore);
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query) {
    String sql = SQLTranslator.translate(query, this.fieldSupplier);
    return getResults(sql, this.datastore.spark, query);
  }

  static Table getResults(String sql, SparkSession sparkSession, DatabaseQuery query) {
    Dataset<Row> ds = sparkSession.sql(sql);
    Pair<List<Field>, List<List<Object>>> result = transform(
            Arrays.stream(ds.schema().fields()).toList(),
            f -> new Field(f.name(), datatypeToClass(f.dataType())),
            ds.toLocalIterator(),
            (i, r) -> r.get(i));
    return new ColumnarTable(
            result.getOne(),
            query.measures,
            IntStream.range(query.select.size(), query.select.size() + query.measures.size()).toArray(),
            IntStream.range(0, query.select.size()).toArray(),
            result.getTwo());
  }

  @Override
  public List<String> supportedAggregationFunctions() {
    return SUPPORTED_AGGREGATION_FUNCTIONS;
  }
}
