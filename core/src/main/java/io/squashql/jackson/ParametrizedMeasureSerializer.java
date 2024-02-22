package io.squashql.jackson;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import io.squashql.query.measure.ParametrizedMeasure;
import io.squashql.query.measure.Repository;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static io.squashql.jackson.JacksonUtil.OBJECT_MAPPER;

@NoArgsConstructor
public class ParametrizedMeasureSerializer extends JsonSerializer<ParametrizedMeasure> {

  @Override
  public void serializeWithType(ParametrizedMeasure value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
    serialize(value, gen, serializers);
  }

  @Override
  public void serialize(ParametrizedMeasure value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
    gen.writeStartObject();
    gen.writeStringField(JsonTypeInfo.Id.CLASS.getDefaultPropertyName(), value.getClass().getName());
    gen.writeStringField("key", value.key);
    gen.writeStringField("alias", value.alias);

    Map<String, JavaType> parameterTypes = new HashMap<>();
    Repository.getParameterTypes(value.key).forEach(pt -> parameterTypes.put(pt.getOne(), pt.getTwo()));
    gen.writeFieldName("parameters");
    gen.writeStartObject();
    for (Map.Entry<String, Object> entry : value.parameters.entrySet()) {
      String propertyKey = entry.getKey();
      JavaType javaType = parameterTypes.get(propertyKey);
      if (javaType == null) {
        throw new IllegalArgumentException("Incorrect key " + propertyKey + ". Expected keys are: " + parameterTypes.keySet());
      }
      gen.writeFieldName(propertyKey);
      OBJECT_MAPPER.writerFor(javaType).writeValue(gen, entry.getValue());
    }
    gen.writeEndObject();
    gen.writeEndObject();
  }
}
