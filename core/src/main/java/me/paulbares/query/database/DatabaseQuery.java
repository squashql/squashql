package me.paulbares.query.database;

import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.AggregatedMeasure;
import me.paulbares.query.ExpressionMeasure;
import me.paulbares.query.Measure;
import me.paulbares.query.UnresolvedExpressionMeasure;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.TableDto;

import java.util.*;

public class DatabaseQuery {

  public TableDto table;
  public Map<String, List<String>> coordinates = new LinkedHashMap<>();
  public Map<String, ConditionDto> conditions = new LinkedHashMap<>();
  public List<Measure> measures = new ArrayList<>();

  /**
   * For Jackson.
   */
  public DatabaseQuery() {
  }

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

  public DatabaseQuery aggregatedMeasure(String field, String agg) {
    return aggregatedMeasure(null, field, agg);
  }

  public DatabaseQuery aggregatedMeasure(String alias, String field, String agg) {
    withMeasure(new AggregatedMeasure(alias, field, agg));
    return this;
  }

  public DatabaseQuery expressionMeasure(String alias, String expression) {
    withMeasure(new ExpressionMeasure(alias, expression));
    return this;
  }

  public DatabaseQuery unresolvedExpressionMeasure(String alias) {
    withMeasure(new UnresolvedExpressionMeasure(alias));
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

  public DatabaseQuery condition(String field, ConditionDto conditionDto) {
    this.conditions.put(field, conditionDto);
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() +
            '{' +
            "table=" + this.table +
            ", coordinates=" + this.coordinates +
            ", conditions=" + this.conditions +
            ", measures=" + this.measures +
            '}';
  }

  public String json() {
    return JacksonUtil.serialize(this);
  }
}
