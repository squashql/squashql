package io.squashql.query.dto;

public enum ConditionType {

  /**
   * Logical AND condition.
   */
  AND("and"),
  /**
   * Logical OR condition.
   */
  OR("or"),
  /**
   * In comparison.
   */
  IN("in"),
  /**
   * Equal.
   */
  EQ("="),
  /**
   * Not Equal.
   */
  NEQ("<>"),
  /**
   * Lower than.
   */
  LT("<"),
  /**
   * Lower.
   */
  LE("<="),
  /**
   * Greater than.
   */
  GT(">"),
  /**
   * Greater.
   */
  GE(">="),
  /**
   * Like.
   */
  LIKE("like"),
  /**
   * Returns true if the array contains the element.
   */
  ARRAY_CONTAINS("array_contains"),
  /**
   * Static condition, true only.
   */
  TRUE("true"),
  /**
   * Static condition, false only.
   */
  FALSE("false"),
  /**
   * Is Null
   */
  NULL("is null"),
  /**
   * Is Not Null
   */
  NOT_NULL("is not null");

  public final String sqlInfix;

  ConditionType(String sqlInfix) {
    this.sqlInfix = sqlInfix;
  }
}
