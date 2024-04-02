package io.squashql.query.builder;

import io.squashql.query.field.Field;
import io.squashql.query.dto.ConditionDto;
import io.squashql.query.dto.CriteriaDto;

public interface HasTable extends HasCondition {

  /**
   * Kept for now not to have to rewrite a lot of tests. Yes I am lazy :). Use {@link #where(CriteriaDto)} instead.
   */
  @Deprecated
  HasTable where(Field field, ConditionDto conditionDto);

  HasTable where(CriteriaDto criteriaDto);
}
