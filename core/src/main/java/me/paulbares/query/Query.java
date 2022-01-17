package me.paulbares.query;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import me.paulbares.jackson.ContextValueDeserializer;
import me.paulbares.query.context.ContextValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class Query {

  public static final AtomicLong ID = new AtomicLong();

  public final long id;

  public Map<String, List<String>> coordinates = new LinkedHashMap<>();

  public List<Measure> measures = new ArrayList<>();

  @JsonDeserialize(contentUsing = ContextValueDeserializer.class)
  public Map<String, ContextValue> context = new HashMap<>();

  public Query() {
    this.id = ID.getAndIncrement();
  }

  public Query addWildcardCoordinate(String field) {
    this.coordinates.put(field, null);
    return this;
  }

  public Query addSingleCoordinate(String field, String value) {
    this.coordinates.put(field, List.of(value));
    return this;
  }

  public Query addCoordinates(String field, String first, String... others) {
    List<String> values = new ArrayList<>();
    values.add(first);
    values.addAll(Arrays.stream(others).toList());
    this.coordinates.put(field, values);
    return this;
  }

  public Query addAggregatedMeasure(String field, String agg) {
    this.measures.add(new AggregatedMeasure(field, agg));
    return this;
  }

  public Query addExpressionMeasure(String alias, String expression) {
    this.measures.add(new ExpressionMeasure(alias, expression));
    return this;
  }

  public Query addContext(String key, ContextValue value) {
    this.context.put(key, value);
    return this;
  }

  @Override
  public String toString() {
    return "Query{" +
            "coordinates=" + coordinates +
            ", measures=" + measures +
            ", id=" + id +
            ", context=" + context +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Query query = (Query) o;
    return this.id == query.id && Objects.equals(this.coordinates, query.coordinates) && Objects.equals(this.measures,
            query.measures) && Objects.equals(this.context, query.context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.id, this.coordinates, this.measures, this.context);
  }
}
