package me.paulbares.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import me.paulbares.query.Measure;
import me.paulbares.query.Table;

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

  public static String tableToCsv(Table table) {
    Iterator<List<Object>> it = table.rowIterator();
    List<List<Object>> rows = new ArrayList<>();
    while (it.hasNext()) {
      rows.add(it.next());
    }
    return JacksonUtil.serialize(Map.of("columns", table.headers(), "rows", rows));
  }
}
