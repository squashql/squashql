package io.squashql.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JSR310Module;
import io.squashql.query.Field;
import io.squashql.query.dto.QueryDto;

public class JacksonUtil {

  public static final ObjectMapper OBJECT_MAPPER;

  static {
    OBJECT_MAPPER = new ObjectMapper();
    var simpleModule = new SimpleModule();
    simpleModule.addKeyDeserializer(Field.class, new QueryDto.KeyFieldDeserializer());
    simpleModule.addKeySerializer(Field.class, new QueryDto.KeyFieldSerializer());
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
}
