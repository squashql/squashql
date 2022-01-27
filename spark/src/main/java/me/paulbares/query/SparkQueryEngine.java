package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.dto.JoinDto;
import me.paulbares.dto.QueryDto;
import me.paulbares.dto.TableDto;
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
  protected Table retrieveAggregates(QueryDto query) {
    LOGGER.info("Executing " + query);
    String sql = SQLTranslator.translate(query);
    LOGGER.info("Translated query #" + query + " to " + sql);
    createOrReplaceTempView(query.table);
    Dataset<Row> ds = this.datastore.spark.sql(sql);
    return new DatasetTable(ds);
  }

  protected void createOrReplaceTempView(TableDto table) {
    this.datastore.get(table.name).createOrReplaceTempView(table.name);
    for (JoinDto join : table.joins) {
      this.datastore.get(join.table.name).createOrReplaceTempView(join.table.name);
      createOrReplaceTempView(join.table);
    }
  }
}
