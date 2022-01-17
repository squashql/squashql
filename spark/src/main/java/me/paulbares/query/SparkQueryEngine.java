package me.paulbares.query;

import me.paulbares.SparkDatastore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.jdk.javaapi.CollectionConverters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class SparkQueryEngine implements QueryEngine {

  private static final Logger LOGGER = Logger.getLogger(SparkQueryEngine.class.getName());

  public static final String GRAND_TOTAL = "Grand Total";
  public static final String TOTAL = "Total";

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
    datastore.get().createOrReplaceTempView(SparkDatastore.BASE_STORE_NAME);
    Dataset<Row> ds = datastore.spark.sql(sql);
    return postProcessDataset(ds, query);
  }

  protected Dataset<Row> postProcessDataset(Dataset<Row> dataset, Query query) {
    if (!query.withTotals) {
      return dataset;
    }

    return editTotalsAndSubtotals(dataset, query);
  }

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

    return datastore.spark.createDataFrame(newRows, dataset.schema());
  }

  public Dataset<Row> executeGrouping(ScenarioGroupingQuery query) {
    ComparisonMethod comparisonMethod = query.comparisonMethod;
    Map<String, List<String>> groups = query.groups;

    Query q = new Query().addWildcardCoordinate("scenario");
    q.measures.addAll(query.measures);

    Dataset<Row> raw = executeSpark(q);

    Map<String, Row> rowByScenario = new HashMap<>();
    Iterator<Row> rowIterator = raw.toLocalIterator();
    while (rowIterator.hasNext()) {
      Row row = rowIterator.next();
      rowByScenario.put(row.getString(0), row);
    }

    StructType schema = new StructType().add("group", DataTypes.StringType);
    StructField[] rawFields = raw.schema().fields();
    for (int i = 0; i < rawFields.length; i++) {
      StructField rawField = rawFields[i];
      if (i == 0) {
        schema = schema.add(rawField);
      } else {
        String newName = comparisonMethod.name().substring(0, 3).toLowerCase() + ". diff. " + rawField.name();
        schema = schema.add(newName, rawField.dataType());
      }
    }

    List<Row> newRows = new ArrayList<>();
    Row[] previous = new Row[1];

    groups.forEach((group, scenarios) -> {
      Row base = rowByScenario.get("base");
      previous[0] = null;
      scenarios.forEach(scenario -> {
        Row row = rowByScenario.get(scenario);
        if (row == null) {
          return;
        }

        if (previous[0] == null) {
          previous[0] = base;
        }

        List<Object> elements = new ArrayList<>();
        List<Object> objects = CollectionConverters.asJava(row.toSeq());
        elements.add(group);
        elements.add(objects.get(0));

        for (int i = 1; i < objects.size(); i++) {
          DataType rawField = rawFields[i].dataType();
          Object newValue = switch (comparisonMethod) {
            case ABSOLUTE -> computeAbsoluteDiff(objects.get(i), previous[0].get(i), rawField);
            case RELATIVE -> computeRelativeDiff(objects.get(i), previous[0].get(i), rawField);
          };
          elements.add(newValue);
        }
        Row newRow = RowFactory.create(elements.toArray(new Object[0]));
        newRows.add(newRow);

        previous[0] = rowByScenario.get(scenario);
      });
    });

    return datastore.spark.createDataFrame(newRows, schema);
  }

  private Object computeRelativeDiff(Object current, Object previous, DataType dataType) {
    if (dataType.equals(DataTypes.DoubleType)) {
      return (((double) current) - ((double) previous)) / ((double) previous);
    } else if (dataType.equals(DataTypes.FloatType)) {
      return (((float) current) - ((float) previous)) / ((float) previous);
    } else if (dataType.equals(DataTypes.IntegerType)) {
      return (((int) current) - ((int) previous)) / ((int) previous);
    } else if (dataType.equals(DataTypes.LongType)) {
      return (((long) current) - ((long) previous)) / ((long) previous);
    } else {
      throw new RuntimeException("Unsupported type " + dataType);
    }
  }

  private Object computeAbsoluteDiff(Object current, Object previous, DataType dataType) {
    if (dataType.equals(DataTypes.DoubleType)) {
      return ((double) current) - ((double) previous);
    } else if (dataType.equals(DataTypes.FloatType)) {
      return ((float) current) - ((float) previous);
    } else if (dataType.equals(DataTypes.IntegerType)) {
      return ((int) current) - ((int) previous);
    } else if (dataType.equals(DataTypes.LongType)) {
      return ((long) current) - ((long) previous);
    } else {
      throw new RuntimeException("Unsupported type " + dataType);
    }
  }
}
