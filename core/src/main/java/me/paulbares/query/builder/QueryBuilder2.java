package me.paulbares.query.builder;

import me.paulbares.query.*;
import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.Period;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.TableDto;

import java.util.List;
import java.util.Map;

import static me.paulbares.query.QueryBuilder.periodComparison;

public class QueryBuilder2 implements HasFromTable, HasCondition, HasGroupBy, HasTable {

  private final QueryDto queryDto = new QueryDto();

  public static HasFromTable from(TableDto table) {
    QueryBuilder2 qb = new QueryBuilder2();
    qb.queryDto.table = table;
    return qb;
  }

  @Override
  public HasStartedBuildingJoin join(TableDto tableDto) {
    return null;
  }

  @Override
  public HasTable where(String field, ConditionDto conditionDto) {
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
    TableDto saas = new TableDto("saas");
    TableDto a = new TableDto("a");
    qb
            .from(saas)
            .join(a)
            .on(a.name, "id", saas.name, "id")
//            .on(a.name, "id", saas.name, "id")
            .join(a)
            .on(a.name, "id", saas.name, "id")
            .where(null, null)
            .where(null, null)
            .groupBy(List.of("col1", "col2"), year)
            .select(List.of("col1", "col2"), List.of(year), List.of(growth))

    ;

    System.out.println(qb.queryDto.json());
  }
}
