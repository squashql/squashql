package me.paulbares.query.builder;

import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.CriteriaDto;

public interface HasTable extends HasCondition {

  HasTable where(String field, ConditionDto conditionDto);
  HasTable where(CriteriaDto criteriaDto);
}
