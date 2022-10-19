package me.paulbares.query.builder;

import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.TableDto;

public interface HasFromTable extends HasCondition {
  HasTable where(String field, ConditionDto conditionDto);

  HasStartedBuildingJoin join(TableDto tableDto);
}
