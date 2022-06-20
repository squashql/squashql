package me.paulbares.query.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import me.paulbares.jackson.deserializer.ContextValueDeserializer;
import me.paulbares.query.*;
import me.paulbares.query.context.ContextValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NewQueryDto {

  public TableDto table;

  public List<String> columns = new ArrayList<>();

  public List<ColumnSet> columnSets = new ArrayList<>();

  public List<Measure> measures = new ArrayList<>();

  @JsonDeserialize(contentUsing = ContextValueDeserializer.class)
  public Map<String, ContextValue> context = new HashMap<>();

  /**
   * For Jackson.
   */
  public NewQueryDto() {
  }

  public NewQueryDto withColumn(String column) {
    this.columns.add(column);
    return this;
  }

  public NewQueryDto withColumnSet(ColumnSet columnSet) {
    this.columnSets.add(columnSet);
    return this;
  }

  public NewQueryDto aggregatedMeasure(String field, String agg) {
    withMetric(new AggregatedMeasure(field, agg));
    return this;
  }

  public NewQueryDto expressionMeasure(String alias, String expression) {
    withMetric(new ExpressionMeasure(alias, expression));
    return this;
  }

  public NewQueryDto unresolvedExpressionMeasure(String alias) {
    withMetric(new UnresolvedExpressionMeasure(alias));
    return this;
  }

  public NewQueryDto withMetric(Measure m) {
    this.measures.add(m);
    return this;
  }

  public NewQueryDto context(String key, ContextValue value) {
    this.context.put(key, value);
    return this;
  }

  public NewQueryDto table(TableDto table) {
    table(table.name);
    return this;
  }

  public NewQueryDto table(String tableName) {
    this.table = new TableDto(tableName);
    return this;
  }
}
