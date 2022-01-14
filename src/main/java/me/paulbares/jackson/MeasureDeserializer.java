package me.paulbares.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import me.paulbares.query.AggregatedMeasure;
import me.paulbares.query.ExpressionMeasure;
import me.paulbares.query.Measure;

import java.io.IOException;
import java.util.Objects;

public class MeasureDeserializer extends JsonDeserializer<Measure> {
  @Override
  public Measure deserialize(JsonParser p, DeserializationContext deserializationContext) throws IOException {
    JsonNode treeNode = p.getCodec().readTree(p);
    JsonNode aggregationFunction = treeNode.get("aggregationFunction");
    if (aggregationFunction != null) {
      return new AggregatedMeasure(
              Objects.requireNonNull(treeNode.get("field")).asText(),
              aggregationFunction.asText());
    } else {
      return new ExpressionMeasure(
              Objects.requireNonNull(treeNode.get("alias")).asText(),
              Objects.requireNonNull(treeNode.get("expression")).asText()
      );
    }
  }
}
