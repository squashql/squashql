package me.paulbares.query.dto;

public enum ConditionType {
  /**
   * Logical AND condition.
   */
  AND,
  /**
   * Logical OR condition.
   */
  OR,
  /**
   * In comparison.
   */
  IN,
  /**
   * Equal.
   */
  EQ,
  /**
   * Not Equal.
   */
  NEQ,
  /**
   * Lower than.
   */
  LT,
  /**
   * Lower.
   */
  LE,
  /**
   * Greater than.
   */
  GT,
  /**
   * Greater.
   */
  GE,
  /**
   * Static condition, true only.
   */
  TRUE,
  /**
   * Static condition, false only.
   */
  FALSE,
  /**
   * Is Null
   */
  NULL,
  /**
   * Is Not Null
   */
  NOT_NULL,
}
