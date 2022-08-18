package me.paulbares.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.*;
import me.paulbares.query.context.ContextValue;

import java.util.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class QueryDto {

  public static final String BUCKET = "bucket";
  public static final String PERIOD = "period";

  public TableDto table;

  public List<String> columns = new ArrayList<>();

  public Map<String, ColumnSet> columnSets = new LinkedHashMap<>();

  public List<Measure> measures = new ArrayList<>();

  public Map<String, ConditionDto> conditions = new HashMap<>();

  public Map<String, OrderDto> orders = new HashMap<>();

  public Map<String, ContextValue> context = new HashMap<>();

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

  public QueryDto orderBy(String column, OrderKeywordDto orderKeywordDto) {
    this.orders.put(column, new SimpleOrderDto(orderKeywordDto));
    return this;
  }

  public QueryDto orderBy(String column, List<?> firstElements) {
    this.orders.put(column, new ExplicitOrderDto(firstElements));
    return this;
  }

  public String json() {
    return JacksonUtil.serialize(this);
  }
}
