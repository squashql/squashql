package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.query.context.Totals;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.jdk.javaapi.CollectionConverters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

public class SparkQueryEngine implements QueryEngine {

  private static final Logger LOGGER = Logger.getLogger(SparkQueryEngine.class.getName());

  public final SparkDatastore datastore;

  public SparkQueryEngine(SparkDatastore datastore) {
    this.datastore = datastore;
  }

  @Override
  public Table execute(Query query) {
    return new DatasetTable(executeSpark(query));
  }

  public Dataset<Row> executeSpark(Query query) {
    LOGGER.info("Executing " + query);
    String sql = SQLTranslator.translate(query);
    LOGGER.info("Translated query #" + query.id + " to " + sql);
    this.datastore.get().createOrReplaceTempView(SparkDatastore.BASE_STORE_NAME);
    Dataset<Row> ds = this.datastore.spark.sql(sql);
    return postProcessDataset(ds, query);
  }

  protected Dataset<Row> postProcessDataset(Dataset<Row> dataset, Query query) {
    if (!query.context.containsKey(Totals.KEY)) {
      return dataset;
    }

    return editTotalsAndSubtotals(dataset, query);
  }

  // TODO should be put in core
  protected Dataset<Row> editTotalsAndSubtotals(Dataset<Row> dataset, Query query) {
    Iterator<Row> rowIterator = dataset.toLocalIterator();

    List<Row> newRows = new ArrayList<>((int) dataset.count());
    List<String> headers = new ArrayList<>(query.coordinates.keySet());
    while (rowIterator.hasNext()) {
      Row row = rowIterator.next();
      List<Object> objects = new ArrayList<>(CollectionConverters.asJava(row.toSeq()));

      Object[] newHeaders = new String[headers.size()];
      for (int i = 0; i < headers.size(); i++) {
        Object current = objects.get(i);
        if (i == 0 && current == null) {
          // GT
          newHeaders[i] = GRAND_TOTAL;
        } else if (i >= 1 && objects.get(i - 1) != null && current == null) {
          // Total
          newHeaders[i] = TOTAL;
        } else {
          newHeaders[i] = current; // nothing to change
        }
      }

      for (int i = 0; i < newHeaders.length; i++) {
          objects.set(i, newHeaders[i]);
      }
      Row newRow = RowFactory.create(objects.toArray(new Object[0]));
      newRows.add(newRow);
    }

    return this.datastore.spark.createDataFrame(newRows, dataset.schema());
  }
}
