package me.paulbares.serialization;

import me.paulbares.jackson.JacksonUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.jdk.javaapi.CollectionConverters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SerializationUtils {

  // FIXME might be useless. To check
  public static String datasetToJSON(Dataset<Row> dataset) {
    Iterator<String> it = dataset.toJSON().toLocalIterator();
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    while (it.hasNext()) {
      sb.append(it.next());
      if (it.hasNext()) {
        sb.append(',');
      }
    }
    sb.append(']');
    return sb.toString();
  }

  public static String datasetToCsv(Dataset<Row> dataset) {
    Iterator<Row> it = dataset.toLocalIterator();
    List<List<Object>> rows = new ArrayList<>();
    while (it.hasNext()) {
      Row next = it.next();
      rows.add(CollectionConverters.asJava(next.toSeq()));
    }
    return JacksonUtil.serialize(Map.of("columns", dataset.columns(), "rows", rows));
  }
}
