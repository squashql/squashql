package io.squashql.query.builder;

import io.squashql.query.dto.CriteriaDto;
import io.squashql.query.dto.JoinType;

public class JoinTableBuilder implements HasStartedBuildingJoin {

  final Query parent;
  final String tableName;
  final JoinType joinType;
  CriteriaDto joinCriteriaDto;

  public JoinTableBuilder(Query parent, String tableName, JoinType joinType) {
    this.parent = parent;
    this.tableName = tableName;
    this.joinType = joinType;
  }

  @Override
  public HasJoin on(CriteriaDto joinCriteriaDto) {
    this.joinCriteriaDto = joinCriteriaDto;
    return this.parent;
  }
}
