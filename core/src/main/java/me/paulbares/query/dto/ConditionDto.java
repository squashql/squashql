package me.paulbares.query.dto;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import static me.paulbares.query.dto.ConditionType.*;

/**
 * Marker interface to represent a (logical, value) condition.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public sealed interface ConditionDto permits ConstantConditionDto, InConditionDto, LogicalConditionDto, SingleValueConditionDto {

  ConditionDto NULL_CONDITION = new ConstantConditionDto(NULL);

  ConditionDto NOT_NULL_CONDITION = new ConstantConditionDto(NOT_NULL);

  ConditionType type();

  /**
   * Creates a new condition that represents the result of a logical <b>and</b> between this condition and another
   * condition.
   *
   * @param other the condition to combine to this one
   * @return the new condition representing the result of the and
   */
  default ConditionDto and(ConditionDto other) {
    return new LogicalConditionDto(AND, this, other);
  }

  /**
   * Creates a new condition that represents the result of a logical <b>and</b> between this condition and another
   * condition.
   *
   * @param other the condition to combine to this one
   * @return the new condition representing the result of the and
   */
  default ConditionDto or(ConditionDto other) {
    return new LogicalConditionDto(OR, this, other);
  }
}
