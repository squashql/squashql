package me.paulbares.jackson.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.ConditionType;
import me.paulbares.query.dto.LogicalConditionDto;
import me.paulbares.query.dto.SingleValueConditionDto;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static me.paulbares.query.dto.ConditionType.AND;
import static me.paulbares.query.dto.ConditionType.OR;

/**
 * {@link JsonDeserializer} for {@link ConditionDto}.
 */
public class ConditionDeserializer extends JsonDeserializer<ConditionDto> {

  @Override
  public ConditionDto deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    final JsonNode node = p.getCodec().readTree(p);
    final ConditionType type = getType(node.get("type").asText());
    return switch (type) {
      case AND -> {
        ConditionDto one = p.getCodec().treeToValue(node.get("one"), ConditionDto.class);
        ConditionDto two = p.getCodec().treeToValue(node.get("two"), ConditionDto.class);
        yield new LogicalConditionDto(AND, one, two);
      }
      case OR -> {
        ConditionDto one = p.getCodec().treeToValue(node.get("one"), ConditionDto.class);
        ConditionDto two = p.getCodec().treeToValue(node.get("two"), ConditionDto.class);
        yield new LogicalConditionDto(OR, one, two);
      } case IN -> {
        Iterator<JsonNode> value = node.get("value").elements();
        Set<Object> set = new HashSet<>();
        while(value.hasNext()) {
          Object o = p.getCodec().treeToValue(value.next(), Object.class);
          set.add(o);
        }
        yield new SingleValueConditionDto(type, set);
      }
      case GE, EQ, NEQ, GT, LE, LT -> {
        JsonNode value = node.get("value");
        Object o = p.getCodec().treeToValue(value, Object.class);
        yield new SingleValueConditionDto(type, o);
      }
      default -> throw new IllegalStateException("Unexpected value: " + type);
    };
  }

  private ConditionType getType(String type) {
    try {
      return ConditionType.valueOf(type.toLowerCase());
    } catch (IllegalArgumentException e) {
      return ConditionType.valueOf(type.toUpperCase());
    }
  }
}
