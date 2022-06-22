package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.query.dto.QueryDto;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.logging.Logger;
import java.util.stream.IntStream;

public class SparkQueryEngine extends AQueryEngine<SparkDatastore> {

  private static final Logger LOGGER = Logger.getLogger(SparkQueryEngine.class.getName());

  public SparkQueryEngine(SparkDatastore datastore) {
    super(datastore);
  }

  @Override
  protected Table retrieveAggregates(QueryDto query) {
    LOGGER.fine("Executing " + query);
    String sql = SQLTranslator.translate(query, this.fieldSupplier);
    LOGGER.fine("Translated query #" + query + " to " + sql);
    Dataset<Row> ds = this.datastore.spark.sql(sql);
    return new DatasetTable(ds,
            query.measures,
            IntStream.range(query.coordinates.size(), query.coordinates.size() + query.measures.size()).toArray(),
            IntStream.range(0, query.coordinates.size()).toArray(),
            this.datastore.storesByName().get(query.table.name).scenarioFieldName());
  }
}
