package me.paulbares.jackson.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.LogicalConditionDto;
import me.paulbares.query.dto.SingleValueConditionDto;

import java.io.IOException;

/**
 * {@link JsonSerializer} for {@link ConditionDto}.
 */
public class ConditionSerializer extends JsonSerializer<ConditionDto> {

  @Override
  public void serialize(ConditionDto value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
    gen.writeStartObject();
    gen.writeStringField("type", value.type().name().toLowerCase());
    switch (value.type()) {
    case IN, LT, LE, GT, GE, EQ, NEQ -> gen.writeObjectField("value", ((SingleValueConditionDto) value).value);
    case AND, OR -> {
      gen.writeObjectField("one", ((LogicalConditionDto) value).one);
      gen.writeObjectField("two", ((LogicalConditionDto) value).one);
    }
    default -> throw new IllegalStateException("Unexpected value: " + value.type());
    }
    gen.writeEndObject();
  }
}
