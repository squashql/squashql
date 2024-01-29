package io.squashql.query.builder;

import io.squashql.query.ColumnSet;
import io.squashql.query.Field;
import io.squashql.query.Measure;
import io.squashql.query.NamedField;
import io.squashql.query.dto.*;

import java.util.Arrays;
import java.util.List;

public class Query implements HasCondition, HasHaving, HasJoin, HasStartedBuildingTable, HasOrderBy, CanAddRollup {

  final QueryDto queryDto = new QueryDto();

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
  public HasStartedBuildingJoin join(String tableName, JoinType joinType) {
    addJoinToQueryDto();
    this.currentJoinTableBuilder = new JoinTableBuilder(this, tableName, joinType);
    return this.currentJoinTableBuilder;
  }

  @Override
  public HasStartedBuildingJoin join(VirtualTableDto virtualTableDto, JoinType joinType) {
    addJoinToQueryDto();
    this.queryDto.virtualTableDtos.add(virtualTableDto);
    this.currentJoinTableBuilder = new JoinTableBuilder(this, virtualTableDto.name, JoinType.INNER);
    return this.currentJoinTableBuilder;
  }

  void addJoinToQueryDto() {
    JoinTableBuilder jtb = this.currentJoinTableBuilder;
    if (jtb != null) {
      this.queryDto.table.join(new TableDto(jtb.tableName), jtb.joinType, jtb.joinCriteriaDto);
      this.currentJoinTableBuilder = null;
    }
  }

  @Override
  public HasTable where(Field field, ConditionDto conditionDto) {
    addJoinToQueryDto();
    this.queryDto.withCondition(field, conditionDto);
    return this;
  }

  @Override
  public HasTable where(CriteriaDto criteriaDto) {
    addJoinToQueryDto();
    this.queryDto.withWhereCriteria(criteriaDto);
    return this;
  }

  @Override
  public CanAddRollup select(List<? extends Field> columns, List<ColumnSet> columnSets, List<Measure> measures) {
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
  public HasHaving orderBy(NamedField column, OrderKeywordDto orderKeywordDto) {
    this.queryDto.orderBy(column, orderKeywordDto);
    return this;
  }

  @Override
  public HasHaving orderBy(NamedField column, List<?> firstElements) {
    this.queryDto.orderBy(column, firstElements);
    return this;
  }

  @Override
  public CanAddHaving rollup(Field... columns) {
    Arrays.stream(columns).forEach(this.queryDto::withRollup);
    return this;
  }

  @Override
  public HasHaving having(CriteriaDto criteriaDto) {
    this.queryDto.withHavingCriteria(criteriaDto);
    return this;
  }

  @Override
  public QueryDto build() {
    return this.queryDto;
  }
}
