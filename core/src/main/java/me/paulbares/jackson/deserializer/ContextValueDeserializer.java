package me.paulbares.jackson.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import me.paulbares.query.context.ContextValue;
import me.paulbares.query.context.Totals;

import java.io.IOException;
import java.util.Objects;

public class ContextValueDeserializer extends JsonDeserializer<ContextValue> {
  @Override
  public ContextValue deserialize(JsonParser p, DeserializationContext deserializationContext) throws IOException {
    JsonNode treeNode = p.getCodec().readTree(p);
    if (Totals.KEY.equals(p.currentName())) {
      return new Totals(treeNode.get("position").asText());
    } else {
      throw new IllegalArgumentException("current: " + p.currentName() + "; " + treeNode.toString());
    }
  }
}
