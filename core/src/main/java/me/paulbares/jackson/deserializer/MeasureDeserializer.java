package me.paulbares.jackson.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import me.paulbares.query.*;

import java.io.IOException;
import java.util.Objects;

public class MeasureDeserializer extends JsonDeserializer<Measure> {
  @Override
  public Measure deserialize(JsonParser p, DeserializationContext deserializationContext) throws IOException {
    JsonNode treeNode = p.getCodec().readTree(p);
    JsonNode aggregationFunction = treeNode.get("aggregation_function");
    if (aggregationFunction != null) {
      return new AggregatedMeasure(
              Objects.requireNonNull(treeNode.get("field")).asText(),
              aggregationFunction.asText());
    }

    JsonNode type = treeNode.get("type");
    if (type != null) {
      if (!type.asText().equals(ComparisonMeasure.KEY)) {
        throw new IllegalArgumentException(type + " type unknown");
      }

      return p.getCodec().treeToValue(treeNode, ComparisonMeasure.class);
    }

    JsonNode expression = treeNode.get("expression");
    if (expression != null) {
      return new ExpressionMeasure(
              Objects.requireNonNull(treeNode.get("alias")).asText(),
              Objects.requireNonNull(expression).asText()
      );
    } else {
      return new UnresolvedExpressionMeasure(Objects.requireNonNull(treeNode.get("alias")).asText());
    }
  }
}
