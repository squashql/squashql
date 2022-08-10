package me.paulbares.query.database;

import lombok.extern.slf4j.Slf4j;
import me.paulbares.SparkDatastore;
import me.paulbares.query.ColumnarTable;
import me.paulbares.query.Table;
import me.paulbares.store.Field;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.eclipse.collections.api.tuple.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static me.paulbares.SparkUtil.datatypeToClass;

@Slf4j
public class SparkQueryEngine extends AQueryEngine<SparkDatastore> {

  public SparkQueryEngine(SparkDatastore datastore) {
    super(datastore);
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query) {
    String sql = SQLTranslator.translate(query, null, this.fieldSupplier);
    log.debug("Translated query " + sql);
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
