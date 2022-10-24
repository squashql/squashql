package me.paulbares.query.builder;

import me.paulbares.query.dto.JoinMappingDto;
import me.paulbares.query.dto.JoinType;

import java.util.ArrayList;
import java.util.List;

public class JoinTableBuilder implements HasStartedBuildingJoin {

  final QueryBuilder2 parent;
  final String tableName;
  final JoinType joinType;

  final List<JoinMappingDto> mappingDtos = new ArrayList<>();

  public JoinTableBuilder(QueryBuilder2 parent, String tableName, JoinType joinType) {
    this.parent = parent;
    this.tableName = tableName;
    this.joinType = joinType;
  }

  @Override
  public HasJoin on(String fromTable, String from, String toTable, String to) {
    this.mappingDtos.add(new JoinMappingDto(fromTable, from, toTable, to));
    return this.parent;
  }
}
