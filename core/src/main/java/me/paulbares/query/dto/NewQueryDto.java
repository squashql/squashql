package me.paulbares.query.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.jackson.deserializer.ContextValueDeserializer;
import me.paulbares.query.*;
import me.paulbares.query.context.ContextValue;

import java.util.*;

public class NewQueryDto {

  public static final String BUCKET = "bucket";
  public static final String PERIOD = "period";

  public TableDto table;

  public List<String> columns = new ArrayList<>();

//  @JsonTypeInfo(include = JsonTypeInfo.As.WRAPPER_OBJECT, use = JsonTypeInfo.Id.NAME)
//  @JsonSubTypes({
//          @JsonSubTypes.Type(value = BucketColumnSetDto.class, name = BUCKET),
//          @JsonSubTypes.Type(value = PeriodColumnSetDto.class, name = PERIOD),
//  })
//  @JsonUnwrapped
  public Map<String, ColumnSet> columnSets = new LinkedHashMap<>();

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

  public NewQueryDto withColumnSet(String type, ColumnSet columnSet) {
    this.columnSets.put(type, columnSet);
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
    this.table = table;
    return this;
  }

  public NewQueryDto table(String tableName) {
    table(new TableDto(tableName));
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NewQueryDto that = (NewQueryDto) o;
    return Objects.equals(this.table, that.table) && Objects.equals(this.columns, that.columns) && Objects.equals(this.columnSets, that.columnSets) && Objects.equals(this.measures, that.measures) && Objects.equals(this.context, that.context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.table, this.columns, this.columnSets, this.measures, this.context);
  }

  public String json() {
    return JacksonUtil.serialize(this);
  }
}
