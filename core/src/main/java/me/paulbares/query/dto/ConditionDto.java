package me.paulbares.query.dto;

import static me.paulbares.query.dto.ConditionType.AND;
import static me.paulbares.query.dto.ConditionType.OR;

/**
 * Marker interface to represent a (logical, value) condition.
 */
public sealed interface ConditionDto permits LogicalConditionDto, SingleValueConditionDto {

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