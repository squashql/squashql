package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.query.dto.JoinDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.TableDto;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.logging.Logger;

public class SparkQueryEngine extends AQueryEngine {

  private static final Logger LOGGER = Logger.getLogger(SparkQueryEngine.class.getName());

  public final SparkDatastore sparkDatastore;

  public SparkQueryEngine(SparkDatastore datastore) {
    super(datastore);
    this.sparkDatastore = datastore;
  }

  @Override
  protected Table retrieveAggregates(QueryDto query) {
    LOGGER.fine("Executing " + query);
    String sql = SQLTranslator.translate(query, this.fieldSupplier);
    LOGGER.fine("Translated query #" + query + " to " + sql);
    createOrReplaceTempView(query.table);
    Dataset<Row> ds = this.sparkDatastore.spark.sql(sql);
    return new DatasetTable(ds, this.sparkDatastore.stores.get(query.table.name).scenarioFieldName());
  }

  protected void createOrReplaceTempView(TableDto table) {
    this.sparkDatastore.get(table.name).createOrReplaceTempView(table.name);
    for (JoinDto join : table.joins) {
      this.sparkDatastore.get(join.table.name).createOrReplaceTempView(join.table.name);
      createOrReplaceTempView(join.table);
    }
  }
}
