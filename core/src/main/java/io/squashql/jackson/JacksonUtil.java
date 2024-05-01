package io.squashql.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.datatype.jsr310.JSR310Module;
import io.squashql.query.Field;
import io.squashql.query.dto.QueryDto;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JacksonUtil {

  public static final ObjectMapper OBJECT_MAPPER;

  static {
    OBJECT_MAPPER = new ObjectMapper();
    var simpleModule = new SimpleModule();
    simpleModule.addKeyDeserializer(Field.class, new QueryDto.KeyFieldDeserializer());
    simpleModule.addKeySerializer(Field.class, new QueryDto.KeyFieldSerializer());
    // Order can matter. For instance when we use Jackson to create a copy of QueryDto (cf. Pivot Table)
    simpleModule.addAbstractTypeMapping(Map.class, LinkedHashMap.class);
    OBJECT_MAPPER.registerModule(simpleModule);
    OBJECT_MAPPER.registerModule(new JSR310Module());
  }

  public static String serialize(Object any) {
    try {
      return OBJECT_MAPPER.writeValueAsString(any);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T deserialize(String json, Class<T> target) {
    try {
      return OBJECT_MAPPER.readValue(json, target);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static JavaType getListJavaType(Class<?> clazz) {
    return TypeFactory.defaultInstance().constructCollectionType(List.class, clazz);
  }

  public static JavaType getJavaType(Class<?> clazz) {
    return TypeFactory.defaultInstance().constructType(clazz);
  }
}
