package me.paulbares.query.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.jackson.deserializer.ContextValueDeserializer;
import me.paulbares.query.AggregatedMeasure;
import me.paulbares.query.ExpressionMeasure;
import me.paulbares.query.Measure;
import me.paulbares.query.UnresolvedExpressionMeasure;
import me.paulbares.query.context.ContextValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS;

public class QueryDto {

  @JsonInclude(ALWAYS)
  public Map<String, List<String>> coordinates = new LinkedHashMap<>();

  public Map<String, ConditionDto> conditions = new LinkedHashMap<>();

  public List<Measure> measures = new ArrayList<>();

  public TableDto table;

  @JsonDeserialize(contentUsing = ContextValueDeserializer.class)
  public Map<String, ContextValue> context = new HashMap<>();

  /**
   * For Jackson.
   */
  public QueryDto() {
  }

  public QueryDto wildcardCoordinate(String field) {
    this.coordinates.put(field, null);
    return this;
  }

  public QueryDto coordinate(String field, String value) {
    coordinates(field, value);
    return this;
  }

  public QueryDto coordinates(String field, String first, String... others) {
    List<String> values = new ArrayList<>();
    values.add(first);
    if (others != null) {
      values.addAll(Arrays.stream(others).toList());
    }
    this.coordinates.put(field, values);
    return this;
  }

  public QueryDto aggregatedMeasure(String field, String agg) {
    this.measures.add(new AggregatedMeasure(field, agg));
    return this;
  }

  public QueryDto expressionMeasure(String alias, String expression) {
    this.measures.add(new ExpressionMeasure(alias, expression));
    return this;
  }

  public QueryDto unresolvedExpressionMeasure(String alias) {
    this.measures.add(new UnresolvedExpressionMeasure(alias));
    return this;
  }

  public QueryDto context(String key, ContextValue value) {
    this.context.put(key, value);
    return this;
  }

  public QueryDto table(TableDto table) {
    this.table = table;
    return this;
  }

  public QueryDto table(String tableName) {
    this.table = new TableDto(tableName);
    return this;
  }

  public QueryDto condition(String field, ConditionDto conditionDto) {
    this.conditions.put(field, conditionDto);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    QueryDto queryDto = (QueryDto) o;
    return Objects.equals(this.coordinates, queryDto.coordinates) && Objects.equals(this.conditions, queryDto.conditions) && Objects.equals(this.measures, queryDto.measures) && Objects.equals(this.table, queryDto.table) && Objects.equals(this.context, queryDto.context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.coordinates, this.conditions, this.measures, this.table, this.context);
  }

  @Override
  public String toString() {
    return "QueryDto{" +
            "coordinates=" + coordinates +
            ", conditions=" + conditions +
            ", measures=" + measures +
            ", table=" + table +
            ", context=" + context +
            '}';
  }

  public String json() {
    return JacksonUtil.serialize(this);
  }
}
