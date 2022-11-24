package me.paulbares.query.builder;

import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.CriteriaDto;

public interface HasTable extends HasCondition {

  /**
   * Kept for now not to have to rewrite a lot of tests. Yes I am lazy :). Use {@link #where(CriteriaDto)} instead.
   */
  @Deprecated
  HasTable where(String field, ConditionDto conditionDto);

  HasTable where(CriteriaDto criteriaDto);
}
