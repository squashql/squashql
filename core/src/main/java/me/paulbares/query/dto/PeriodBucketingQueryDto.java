package me.paulbares.query.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.jackson.deserializer.ContextValueDeserializer;
import me.paulbares.query.Measure;
import me.paulbares.query.context.ContextValue;

import java.util.*;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS;

public class PeriodBucketingQueryDto {

  @JsonInclude(ALWAYS)
  public Map<String, List<String>> coordinates = new LinkedHashMap<>();

  @JsonTypeInfo(include = JsonTypeInfo.As.WRAPPER_OBJECT, use = JsonTypeInfo.Id.NAME)
  @JsonSubTypes({
          @JsonSubTypes.Type(value = Period.Month.class, name = Period.Quarter.JSON_KEY),
          @JsonSubTypes.Type(value = Period.Quarter.class, name = Period.Quarter.JSON_KEY),
          @JsonSubTypes.Type(value = Period.Semester.class, name = Period.Semester.JSON_KEY),
          @JsonSubTypes.Type(value = Period.Year.class, name = Period.Year.JSON_KEY),
  })
  public Period period;

  public TableDto table;

  public List<Measure> measures = new ArrayList<>();

  @JsonDeserialize(contentUsing = ContextValueDeserializer.class)
  public Map<String, ContextValue> context = new HashMap<>();

  public PeriodBucketingQueryDto wildcardCoordinate(String field) {
    this.coordinates.put(field, null);
    return this;
  }

  public PeriodBucketingQueryDto table(String tableName) {
    this.table = new TableDto(tableName);
    return this;
  }

  public PeriodBucketingQueryDto table(TableDto table) {
    this.table = table;
    return this;
  }

  public PeriodBucketingQueryDto context(String key, ContextValue value) {
    this.context.put(key, value);
    return this;
  }

  public PeriodBucketingQueryDto period(Period period) {
    this.period = period;
    return this;
  }

  public PeriodBucketingQueryDto withMeasure(Measure measure) {
    this.measures.add(measure);
    return this;
  }

  public String json() {
    return JacksonUtil.serialize(this);
  }
}
