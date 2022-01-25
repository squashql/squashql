package me.paulbares.query;

import me.paulbares.SparkDatastore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.logging.Logger;

public class SparkQueryEngine extends AQueryEngine {

  private static final Logger LOGGER = Logger.getLogger(SparkQueryEngine.class.getName());

  public final SparkDatastore datastore;

  public SparkQueryEngine(SparkDatastore datastore) {
    this.datastore = datastore;
  }

  @Override
  protected Table retrieveAggregates(Query query) {
    LOGGER.info("Executing " + query);
    String sql = SQLTranslator.translate(query);
    LOGGER.info("Translated query #" + query.id + " to " + sql);
    this.datastore.get(query).createOrReplaceTempView(SparkDatastore.BASE_STORE_NAME);
    Dataset<Row> ds = this.datastore.spark.sql(sql);
    return new DatasetTable(ds);
  }
}
