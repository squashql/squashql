package me.paulbares.jackson.deserializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import me.paulbares.query.ColumnSet;
import me.paulbares.query.dto.BucketColumnSetDto;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.Period;
import me.paulbares.query.dto.PeriodColumnSetDto;
import org.eclipse.collections.api.tuple.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * {@link JsonDeserializer} for {@link ConditionDto}.
 */
public class ColumnSetSerializer extends JsonSerializer<ColumnSet> {

  @Override
  public void serialize(ColumnSet columnSet, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
    if (columnSet instanceof PeriodColumnSetDto pcs) {
      Period period = pcs.period;
      jsonGenerator.writeObject(Map.of(period.getJsonKey(), period));
    } else if (columnSet instanceof BucketColumnSetDto bcs) {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("name", bcs.name);
      jsonGenerator.writeStringField("field", bcs.field);
      jsonGenerator.writeFieldName("values");
      jsonGenerator.writeStartArray();
      for (Pair<String, List<String>> value : bcs.values) {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeObjectField(value.getOne(), value.getTwo());
        jsonGenerator.writeEndObject();
      }
      jsonGenerator.writeEndArray();
      jsonGenerator.writeEndObject();
    } else {
      throw new IllegalArgumentException("unexpected type: " + columnSet.getClass());
    }
  }
}
