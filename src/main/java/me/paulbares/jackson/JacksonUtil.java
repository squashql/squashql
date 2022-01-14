package me.paulbares.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import me.paulbares.query.Measure;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.jdk.javaapi.CollectionConverters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JacksonUtil {

  public static final ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    var simpleModule = new SimpleModule();
    simpleModule.addDeserializer(Measure.class, new MeasureDeserializer());
    mapper.registerModule(simpleModule);
  }

  public static String serialize(Object any) {
    try {
      return mapper.writeValueAsString(any);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T deserialize(String json, Class<T> target) {
    try {
      return mapper.readValue(json, target);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

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
