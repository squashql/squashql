package io.squashql.query.builder;

import io.squashql.query.Field;
import io.squashql.query.TableField;
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
    Field f1 = joinCriteriaDto.field;
    Field f2 = joinCriteriaDto.fieldOther;
    if (f1 instanceof TableField tf1 && f2 instanceof TableField tf2) {
      this.mappingDtos.add(new JoinMappingDto(tf1.name, tf2.name, joinCriteriaDto.conditionType));
    } else {
      throw new IllegalArgumentException("Unexpected field types in join condition. Only regular fields can be used not: "
              + f1 + " - " + f2);
    }
  }
}
