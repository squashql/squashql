package io.squashql.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import io.squashql.query.measure.ParametrizedMeasure;
import io.squashql.query.measure.Repository;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static io.squashql.jackson.JacksonUtil.OBJECT_MAPPER;

@NoArgsConstructor
public class ParametrizedMeasureDeserializer extends JsonDeserializer<ParametrizedMeasure> {

  @Override
  public ParametrizedMeasure deserialize(JsonParser p, DeserializationContext dc) throws IOException {
    JsonNode node = p.getCodec().readTree(p);

    Map<String, JavaType> parameterTypes = new HashMap<>();
    String key = node.get("key").asText();
    Repository.getParameterTypes(key).forEach(pt -> parameterTypes.put(pt.getOne(), pt.getTwo()));

    Map<String, Object> parameters = new HashMap<>();
    for (Map.Entry<String, JsonNode> property : node.get("parameters").properties()) {
      String propertyKey = property.getKey();
      JavaType javaType = parameterTypes.get(propertyKey);
      if (javaType == null) {
        throw new IllegalArgumentException("Incorrect key " + propertyKey + ". Expected keys are: " + parameterTypes.keySet());
      }
      parameters.put(propertyKey, OBJECT_MAPPER.readerFor(javaType).readValue(property.getValue()));
    }

    String alias = node.get("alias").asText();
    return new ParametrizedMeasure(alias, key, parameters);
  }
}
