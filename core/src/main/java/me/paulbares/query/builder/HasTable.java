package me.paulbares.query.builder;

import me.paulbares.query.dto.ConditionDto;

public interface HasTable extends HasCondition {

  HasTable where(String field, ConditionDto conditionDto);
}
