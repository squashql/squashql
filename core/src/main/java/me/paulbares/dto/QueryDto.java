package me.paulbares.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import me.paulbares.jackson.ContextValueDeserializer;
import me.paulbares.query.AggregatedMeasure;
import me.paulbares.query.ExpressionMeasure;
import me.paulbares.query.Measure;
import me.paulbares.query.context.ContextValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class QueryDto {

  public Map<String, List<String>> coordinates = new LinkedHashMap<>();

  public List<Measure> measures = new ArrayList<>();

  public TableDto table;

  @JsonDeserialize(contentUsing = ContextValueDeserializer.class)
  public Map<String, ContextValue> context = new HashMap<>();

  /**
   * For Jackson.
   */
  public QueryDto() {
  }

  public QueryDto addWildcardCoordinate(String field) {
    this.coordinates.put(field, null);
    return this;
  }

  public QueryDto addSingleCoordinate(String field, String value) {
    this.coordinates.put(field, List.of(value));
    return this;
  }

  public QueryDto addCoordinates(String field, String first, String... others) {
    List<String> values = new ArrayList<>();
    values.add(first);
    values.addAll(Arrays.stream(others).toList());
    this.coordinates.put(field, values);
    return this;
  }

  public QueryDto addAggregatedMeasure(String field, String agg) {
    this.measures.add(new AggregatedMeasure(field, agg));
    return this;
  }

  public QueryDto addExpressionMeasure(String alias, String expression) {
    this.measures.add(new ExpressionMeasure(alias, expression));
    return this;
  }

  public QueryDto addContext(String key, ContextValue value) {
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

  @Override
  public String toString() {
    return "QueryDto{" +
            "coordinates=" + coordinates +
            ", measures=" + measures +
            ", table=" + table +
            ", context=" + context +
            '}';
  }
}
