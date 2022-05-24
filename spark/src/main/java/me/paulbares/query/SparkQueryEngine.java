package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.query.dto.QueryDto;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.logging.Logger;

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
    return new DatasetTable(ds, this.datastore.storesByName().get(query.table.name).scenarioFieldName());
  }
}
