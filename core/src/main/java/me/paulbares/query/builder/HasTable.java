package me.paulbares.query.builder;

import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.CriteriaDto;

public interface HasTable extends HasCondition {

  HasTable where(String field, ConditionDto conditionDto); // FIXME should not be possible to call where(CriteriaDto criteriaDto) after
  HasCondition where(CriteriaDto criteriaDto); // FIXME should not be possible to call where(String field, ConditionDto conditionDto); after
}
