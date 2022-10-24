package me.paulbares.query.builder;

import me.paulbares.query.dto.ConditionDto;

public interface HasFromTable extends HasCondition {
  HasTable where(String field, ConditionDto conditionDto);
  HasStartedBuildingJoin leftOuterJoin(String tableName);
  HasStartedBuildingJoin innerJoin(String tableName);
}
