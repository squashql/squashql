package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.query.database.DatabaseQuery;
import me.paulbares.query.database.SQLTranslator;
import me.paulbares.store.Field;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;
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

    // FIXME code similar to the one for clickhouse. Factorize!
    List<Field> fields = Arrays
            .stream(ds.schema().fields())
            .map(f -> new Field(f.name(), datatypeToClass(f.dataType())))
            .collect(Collectors.toList());
    List<List<Object>> values = new ArrayList<>();
    for (int i = 0; i < fields.size(); i++) {
      values.add(new ArrayList<>());
    }
    Iterator<Row> it = ds.toLocalIterator();
    it.forEachRemaining(r -> {
      for (int i = 0; i < r.size(); i++) {
        values.get(i).add(r.get(i));
      }
    });
    return new ColumnarTable(
            fields,
            query.measures,
            IntStream.range(query.coordinates.size(), query.coordinates.size() + query.measures.size()).toArray(),
            IntStream.range(0, query.coordinates.size()).toArray(),
            values);
  }
}
