package me.paulbares.query.builder;

import me.paulbares.query.*;
import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.dto.*;

import java.util.List;
import java.util.Map;

import static me.paulbares.query.QueryBuilder.periodComparison;

public class QueryBuilder2 implements HasFromTable, HasCondition, HasTable, HasSelect, HasJoin {

  private final QueryDto queryDto = new QueryDto();

  private JoinTableBuilder currentJoinTableBuilder;

  public static HasFromTable from(String tableName) {
    QueryBuilder2 qb = new QueryBuilder2();
    qb.queryDto.table = new TableDto(tableName);
    return qb;
  }

  public static HasFromTable from(QueryDto subQuery) {
    QueryBuilder2 qb = new QueryBuilder2();
    qb.queryDto.subQuery = subQuery;
    return qb;
  }

  @Override
  public HasStartedBuildingJoin left_outer_join(String tableName) {
    return join(tableName, JoinType.LEFT);
  }

  @Override
  public HasStartedBuildingJoin inner_join(String tableName) {
    return join(tableName, JoinType.INNER);
  }

  private HasStartedBuildingJoin join(String tableName, JoinType joinType) {
    addJoinToQueryDto();
    this.currentJoinTableBuilder = new JoinTableBuilder(this, tableName, joinType);
    return this.currentJoinTableBuilder;
  }

  private void addJoinToQueryDto() {
    JoinTableBuilder jtb = this.currentJoinTableBuilder;
    if (jtb != null) {
      this.queryDto.table.join(new TableDto(jtb.tableName), jtb.joinType, jtb.mappingDtos);
    }
  }

  @Override
  public HasJoin on(String fromTable, String from, String toTable, String to) {
    this.currentJoinTableBuilder.on(fromTable, from, toTable, to);
    return this;
  }

  @Override
  public HasTable where(String field, ConditionDto conditionDto) {
    addJoinToQueryDto();
    this.queryDto.withCondition(field, conditionDto);
    return this;
  }

  @Override
  public HasSelect select(List<String> columns, List<ColumnSet> columnSets, List<Measure> measures) {
    addJoinToQueryDto();
    columns.forEach(this.queryDto::withColumn);
    columnSets.forEach(cs -> this.queryDto.withColumnSet(cs.getColumnSetKey(), cs));
    measures.forEach(this.queryDto::withMeasure);
    return this;
  }

  @Override
  public QueryDto build() {
    return this.queryDto;
  }

  // FIXME orderBy

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
            .from("saas")
            .inner_join(a.name)
            .on(a.name, "id", saas.name, "id")
//            .on(a.name, "id", saas.name, "id")
            .left_outer_join(a.name)
            .on(a.name, "id", saas.name, "id")
            .where(null, null)
            .where(null, null)
            .select(List.of("col1", "col2"), List.of(year), List.of(growth))

    ;

    System.out.println(qb.queryDto.json());
  }
}
