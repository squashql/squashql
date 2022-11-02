package me.paulbares.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import me.paulbares.query.Table;
import me.paulbares.store.Field;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JacksonUtil {

  public static final ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    var simpleModule = new SimpleModule();
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

  public static Map<String, Iterable<?>> serializeTable(Table table) {
    List<String> fields = table.headers().stream().map(Field::name).collect(Collectors.toList());
    // Jackson can serialize Iterable<?> so there is nothing to do to serialize table!
    return Map.of("columns", fields, "rows", table);
  }
}
