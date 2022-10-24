package me.paulbares.query.builder;

import me.paulbares.query.dto.ConditionDto;

public interface HasFromTable extends HasCondition {
  HasTable where(String field, ConditionDto conditionDto);
  HasStartedBuildingJoin left_outer_join(String tableName);
  HasStartedBuildingJoin inner_join(String tableName);
}
