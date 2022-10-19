package me.paulbares.query;

import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.Period;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.TableDto;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static me.paulbares.query.QueryBuilder.periodComparison;

interface HasFrom extends HasCondition {
  HasFrom where(String field, ConditionDto conditionDto);
}

interface HasCondition {
  HasGroupBy groupBy(List<String> columns, ColumnSet... columnSets);
}

interface HasGroupBy {
  default void select(List<String> columns, List<Measure> measures) {
    select(columns, Collections.emptyList(), measures);
  }

  void select(List<String> columns, List<ColumnSet> columnSets, List<Measure> measures);
}

public class QueryBuilder2 implements HasFrom, HasCondition, HasGroupBy {

  private final QueryDto queryDto = new QueryDto();

  public HasFrom from(TableDto table) {
    this.queryDto.table = table;
    return this;
  }

  @Override
  public HasFrom where(String field, ConditionDto conditionDto) {
    this.queryDto.withCondition(field, conditionDto);
    return this;
  }

  @Override
  public HasGroupBy groupBy(List<String> columns, ColumnSet... columnSets) {
    for (String column : columns) {
      this.queryDto.withColumn(column);
    }

    for (ColumnSet columnSet : columnSets) {
      this.queryDto.withColumnSet(columnSet.getColumnSetKey(), columnSet);
    }
    return this;
  }

  @Override
  public void select(List<String> columns, List<ColumnSet> columnSets, List<Measure> measures) {
    columns.forEach(this.queryDto::withColumn);
    columnSets.forEach(cs -> this.queryDto.withColumnSet(cs.getColumnSetKey(), cs));
    measures.forEach(this.queryDto::withMeasure);
  }

  public static void main(String[] args) {
    ColumnSet year = QueryBuilder.createPeriodColumnSet(new Period.Year("Year"));
    Measure sales = new AggregatedMeasure("sales", "Amount", AggregationFunction.SUM, "Income/Expense", QueryBuilder.eq("Revenue"));
    Measure growth = periodComparison(
            "Growth",
            ComparisonMethod.DIVIDE,
            sales,
            Map.of("Year", "y-1"));

    QueryBuilder2 qb = new QueryBuilder2();
    qb
            .from(new TableDto("saas"))
            .where(null, null)
            .where(null, null)
            .groupBy(List.of("col1", "col2"), year)
            .select(List.of("col1", "col2"), List.of(year), List.of(growth))

    ;

    System.out.println(qb.queryDto.json());
  }
}
