package me.paulbares.query.database;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.AggregatedMeasure;
import me.paulbares.query.ExpressionMeasure;
import me.paulbares.query.Measure;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.TableDto;

import java.util.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class DatabaseQuery {

  public TableDto table;
  public DatabaseQuery subQuery;
  public Map<String, List<String>> coordinates = new LinkedHashMap<>();
  public Map<String, ConditionDto> conditions = new LinkedHashMap<>();
  public List<Measure> measures = new ArrayList<>();

  public DatabaseQuery wildcardCoordinate(String field) {
    this.coordinates.put(field, null);
    return this;
  }

  public DatabaseQuery coordinate(String field, String value) {
    coordinates(field, value);
    return this;
  }

  public DatabaseQuery coordinates(String field, String first, String... others) {
    List<String> values = new ArrayList<>();
    values.add(first);
    if (others != null) {
      values.addAll(Arrays.stream(others).toList());
    }
    this.coordinates.put(field, values);
    return this;
  }

  public DatabaseQuery aggregatedMeasure(String alias, String field, String agg) {
    withMeasure(new AggregatedMeasure(alias, field, agg));
    return this;
  }

  public DatabaseQuery expressionMeasure(String alias, String expression) {
    withMeasure(new ExpressionMeasure(alias, expression));
    return this;
  }

  public DatabaseQuery withMeasure(Measure m) {
    this.measures.add(m);
    return this;
  }

  public DatabaseQuery table(TableDto table) {
    this.table = table;
    return this;
  }

  public DatabaseQuery table(String tableName) {
    this.table = new TableDto(tableName);
    return this;
  }

  public DatabaseQuery subQuery(DatabaseQuery subQuery) {
    this.subQuery = subQuery;
    return this;
  }

  public DatabaseQuery condition(String field, ConditionDto conditionDto) {
    this.conditions.put(field, conditionDto);
    return this;
  }

  public String json() {
    return JacksonUtil.serialize(this);
  }
}
