package io.squashql.query.builder;

import io.squashql.query.dto.CriteriaDto;
import io.squashql.query.dto.JoinMappingDto;
import io.squashql.query.dto.JoinType;

import java.util.ArrayList;
import java.util.List;

public class JoinTableBuilder implements HasStartedBuildingJoin {

  final Query parent;
  final String tableName;
  final JoinType joinType;
  final List<JoinMappingDto> mappingDtos = new ArrayList<>();

  public JoinTableBuilder(Query parent, String tableName, JoinType joinType) {
    this.parent = parent;
    this.tableName = tableName;
    this.joinType = joinType;
  }

  @Override
  public HasJoin on(CriteriaDto joinCriteriaDto) {
    if (joinCriteriaDto.isCriteria()) {
      joinCriteriaDto.children.forEach(this::add);
    } else {
      add(joinCriteriaDto);
    }
    return this.parent;
  }

  private void add(CriteriaDto joinCriteriaDto) {
    if (!joinCriteriaDto.isJoinCriterion()) {
      throw new IllegalArgumentException("Unexpected criterion :" + joinCriteriaDto);
    }
    this.mappingDtos.add(new JoinMappingDto(joinCriteriaDto.field, joinCriteriaDto.fieldOther, joinCriteriaDto.conditionType));
  }
}
