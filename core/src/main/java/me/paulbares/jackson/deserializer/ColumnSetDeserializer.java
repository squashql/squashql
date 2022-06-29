package me.paulbares.jackson.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import me.paulbares.query.ColumnSet;
import me.paulbares.query.dto.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * {@link JsonDeserializer} for {@link ConditionDto}.
 */
public class ColumnSetDeserializer extends JsonDeserializer<ColumnSet> {

  private static final Map<String, Class<?>> periodClazzByKey = Map.of(
          Period.Month.JSON_KEY, Period.Month.class,
          Period.Quarter.JSON_KEY, Period.Quarter.class,
          Period.Semester.JSON_KEY, Period.Semester.class,
          Period.Year.JSON_KEY, Period.Year.class
  );

  @Override
  public ColumnSet deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    final JsonNode node = p.getCodec().readTree(p);
    String current = p.currentName();
    if (current.equals(NewQueryDto.PERIOD)) {
      Class<?> clazz = periodClazzByKey.get(node.fieldNames().next());
      Period period = (Period) p.getCodec().treeToValue(node.iterator().next(), clazz);
      return new PeriodColumnSetDto(period);
    } else if (current.equals(NewQueryDto.BUCKET)) {
      JsonNode name = node.get("name");
      JsonNode field = node.get("field");
      BucketColumnSetDto bcs = new BucketColumnSetDto(name.asText(), field.asText());
      JsonNode values = node.get("values");
      Iterator<JsonNode> iterator = values.iterator();
      while (iterator.hasNext()) {
        JsonNode next = iterator.next();
        String bucket = next.fieldNames().next();
        JsonNode jsonNode = next.get(bucket);
        List<String> list = p.getCodec().treeToValue(jsonNode, List.class);
        bcs.withNewBucket(bucket, list);
      }
      return bcs;
    } else {
      throw new IllegalArgumentException("unexpected field: " + node);
    }
  }
}
