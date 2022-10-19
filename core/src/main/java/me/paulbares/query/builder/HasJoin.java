package me.paulbares.query.builder;

import me.paulbares.query.dto.ConditionDto;

public interface HasJoin extends HasCondition {
  HasJoin where(String field, ConditionDto conditionDto);
}
