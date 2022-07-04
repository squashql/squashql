package me.paulbares.query.database;

import me.paulbares.SparkDatastore;
import me.paulbares.query.ColumnarTable;
import me.paulbares.query.Table;
import me.paulbares.store.Field;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.eclipse.collections.api.tuple.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import static me.paulbares.SparkUtil.datatypeToClass;

public class SparkQueryEngine extends AQueryEngine<SparkDatastore> {

  private static final Logger LOGGER = Logger.getLogger(SparkQueryEngine.class.getName());

  public SparkQueryEngine(SparkDatastore datastore) {
    super(datastore);
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query) {
    LOGGER.fine("Executing " + query);
    String sql = SQLTranslator.translate(query, null, this.fieldSupplier);
    LOGGER.fine("Translated query #" + query + " to " + sql);
    Dataset<Row> ds = this.datastore.spark.sql(sql);
    Pair<List<Field>, List<List<Object>>> result = transform(
            Arrays.stream(ds.schema().fields()).toList(),
            f -> new Field(f.name(), datatypeToClass(f.dataType())),
            ds.toLocalIterator(),
            (i, r) -> r.get(i));
    return new ColumnarTable(
            result.getOne(),
            query.measures,
            IntStream.range(query.coordinates.size(), query.coordinates.size() + query.measures.size()).toArray(),
            IntStream.range(0, query.coordinates.size()).toArray(),
            result.getTwo());
  }
}
