package me.paulbares.jackson;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.module.SimpleModule;
import me.paulbares.jackson.deserializer.ConditionDeserializer;
import me.paulbares.jackson.deserializer.MeasureDeserializer;
import me.paulbares.query.Measure;
import me.paulbares.query.Table;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.store.Field;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JacksonUtil {

  public static final ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    var simpleModule = new SimpleModule();
    simpleModule.addDeserializer(Measure.class, new MeasureDeserializer());
    mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
//    simpleModule.addSerializer(ConditionDto.class, new ConditionSerializer());
    simpleModule.addDeserializer(ConditionDto.class, new ConditionDeserializer());
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
    List<String> fields = table.headers().stream().map(Field::name).collect(Collectors.toList());
    // Jackson can serialize Iterable<?> so there is nothing to do to serialize table!
    return JacksonUtil.serialize(Map.of("columns", fields, "rows", table));
  }
}
