package me.paulbares.query.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Objects;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.jackson.deserializer.ContextValueDeserializer;
import me.paulbares.query.*;
import me.paulbares.query.context.ContextValue;
import me.paulbares.util.CustomExplicitOrdering;

import java.util.*;

import static me.paulbares.query.dto.OrderDto.DESC;

public class QueryDto {

  public static final String BUCKET = "bucket";
  public static final String PERIOD = "period";

  public TableDto table;

  public List<String> columns = new ArrayList<>();

  public Map<String, ColumnSet> columnSets = new LinkedHashMap<>();

  public List<Measure> measures = new ArrayList<>();

  public Map<String, ConditionDto> conditions = new HashMap<>();

  public Map<String, Comparator<?>> comparators = new HashMap<>();

  @JsonDeserialize(contentUsing = ContextValueDeserializer.class)
  public Map<String, ContextValue> context = new HashMap<>();

  /**
   * For Jackson.
   */
  public QueryDto() {
  }

  public QueryDto withColumn(String column) {
    this.columns.add(column);
    return this;
  }

  public QueryDto withColumnSet(String type, ColumnSet columnSet) {
    this.columnSets.put(type, columnSet);
    return this;
  }

  public QueryDto aggregatedMeasure(String field, String agg) {
    withMeasure(new AggregatedMeasure(field, agg));
    return this;
  }

  public QueryDto aggregatedMeasure(String alias, String field, String agg, String conditionField, ConditionDto conditionDto) {
    withMeasure(new AggregatedMeasure(alias, field, agg, conditionField, conditionDto));
    return this;
  }

  public QueryDto expressionMeasure(String alias, String expression) {
    withMeasure(new ExpressionMeasure(alias, expression));
    return this;
  }

  public QueryDto unresolvedExpressionMeasure(String alias) {
    withMeasure(new UnresolvedExpressionMeasure(alias));
    return this;
  }

  public QueryDto withMeasure(Measure m) {
    this.measures.add(m);
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
    table(new TableDto(tableName));
    return this;
  }

  public QueryDto withCondition(String field, ConditionDto conditionDto) {
    this.conditions.put(field, conditionDto);
    return this;
  }

  public QueryDto orderBy(String column, OrderDto orderDto) {
    Comparator<?> comp = Comparator.naturalOrder();
    this.comparators.put(column, orderDto == DESC ? comp.reversed() : comp);
    return this;
  }

  public QueryDto orderBy(String column, List<?> firstElements) {
    this.comparators.put(column, new CustomExplicitOrdering(firstElements));
    return this;
  }

  public String json() {
    return JacksonUtil.serialize(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    QueryDto queryDto = (QueryDto) o;
    return Objects.equal(this.table, queryDto.table) && Objects.equal(this.columns, queryDto.columns) && Objects.equal(this.columnSets, queryDto.columnSets) && Objects.equal(this.measures, queryDto.measures) && Objects.equal(this.conditions, queryDto.conditions) && Objects.equal(this.context, queryDto.context);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.table, this.columns, this.columnSets, this.measures, this.conditions, this.context);
  }
}
