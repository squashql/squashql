package me.paulbares.query.builder;

import me.paulbares.query.ColumnSet;
import me.paulbares.query.Measure;
import me.paulbares.query.dto.*;

import java.util.Arrays;
import java.util.List;

public class Query implements HasCondition, HasSelectAndRollup, HasJoin, HasStartedBuildingTable, HasOrderBy, CanAddRollup {

  private final QueryDto queryDto = new QueryDto();

  private JoinTableBuilder currentJoinTableBuilder;

  public static HasStartedBuildingTable from(String tableName) {
    Query qb = new Query();
    qb.queryDto.table = new TableDto(tableName);
    return qb;
  }

  public static HasTable from(QueryDto subQuery) {
    Query qb = new Query();
    qb.queryDto.subQuery = subQuery;
    return qb;
  }

  @Override
  public HasStartedBuildingJoin leftOuterJoin(String tableName) {
    return join(tableName, JoinType.LEFT);
  }

  @Override
  public HasStartedBuildingJoin innerJoin(String tableName) {
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
      this.currentJoinTableBuilder = null;
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
  public HasTable where(CriteriaDto criteriaDto) {
    addJoinToQueryDto();
    this.queryDto.criteriaDto.children.add(criteriaDto);
    return this;
  }

  @Override
  public CanAddRollup select(List<String> columns, List<ColumnSet> columnSets, List<Measure> measures) {
    addJoinToQueryDto();
    columns.forEach(this.queryDto::withColumn);
    columnSets.forEach(cs -> this.queryDto.withColumnSet(cs.getColumnSetKey(), cs));
    measures.forEach(this.queryDto::withMeasure);
    return this;
  }

  @Override
  public CanBeBuildQuery limit(int limit) {
    this.queryDto.withLimit(limit);
    return this;
  }

  @Override
  public HasSelectAndRollup orderBy(String column, OrderKeywordDto orderKeywordDto) {
    this.queryDto.orderBy(column, orderKeywordDto);
    return this;
  }

  @Override
  public HasSelectAndRollup orderBy(String column, List<?> firstElements) {
    this.queryDto.orderBy(column, firstElements);
    return this;
  }

  @Override
  public HasSelectAndRollup rollup(String... columns) {
    Arrays.stream(columns).forEach(this.queryDto::withRollup);
    return this;
  }

  @Override
  public QueryDto build() {
    return this.queryDto;
  }
}
