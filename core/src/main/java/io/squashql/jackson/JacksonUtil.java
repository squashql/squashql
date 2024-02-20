package io.squashql.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JSR310Module;
import io.squashql.query.Field;
import io.squashql.query.TableField;
import io.squashql.query.dto.QueryDto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JacksonUtil {

  public static final ObjectMapper OBJECT_MAPPER;

  static {
    OBJECT_MAPPER = new ObjectMapper();
    var simpleModule = new SimpleModule();
    simpleModule.addKeyDeserializer(Field.class, new QueryDto.KeyFieldDeserializer());
    simpleModule.addKeySerializer(Field.class, new QueryDto.KeyFieldSerializer());
    simpleModule.addAbstractTypeMapping(Map.class, HashMap.class); // By default, it is LinkedHashMap. Order does not matter
    OBJECT_MAPPER.registerModule(simpleModule);
    OBJECT_MAPPER.registerModule(new JSR310Module());
  }

  public static void main(String[] args) throws JsonProcessingException {
    List<TableField> m = List.of(new TableField("f1"), new TableField("f2"));
    String serialize = JacksonUtil.serialize(m);
    List deserialize = OBJECT_MAPPER.readValue(serialize, new TypeReference<List<TableField>>(){});
    System.out.println(deserialize);
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
