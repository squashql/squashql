package io.squashql.query.dto;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import static io.squashql.query.dto.ConditionType.*;

/**
 * Marker interface to represent a (logical, value) condition.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public sealed interface ConditionDto permits ConstantConditionDto, InConditionDto, LogicalConditionDto, SingleValueConditionDto {

  ConditionDto NULL_CONDITION = new ConstantConditionDto(NULL);
  ConditionDto NOT_NULL_CONDITION = new ConstantConditionDto(NOT_NULL);

  ConditionType type();
}
