package me.paulbares.query;

import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.LogicalConditionDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.SingleValueConditionDto;
import me.paulbares.query.dto.ConditionType;
import me.paulbares.query.dto.TableDto;

import static me.paulbares.query.dto.ConditionType.AND;
import static me.paulbares.query.dto.ConditionType.OR;

public class QueryBuilder {

  public static QueryDto query() {
    return new QueryDto();
  }

  public static TableDto table(String name) {
    return new TableDto(name);
  }

  public static ConditionDto and(ConditionDto first, ConditionDto second, ConditionDto... others) {
    return merge(AND, first, second, others);
  }

  public static ConditionDto or(ConditionDto first, ConditionDto second, ConditionDto... others) {
    return merge(OR, first, second, others);
  }

  public static ConditionDto merge(ConditionType type, ConditionDto first, ConditionDto second, ConditionDto... others) {
    LogicalConditionDto c = new LogicalConditionDto(type, first, second);
    if (others != null) {
      for (ConditionDto other : others) {
        c = new LogicalConditionDto(type, c, other);
      }
    }
    return c;
  }

  public static ConditionDto eq(Object value) {
    return new SingleValueConditionDto(ConditionType.EQ, value);
  }

  public static ConditionDto neq(Object value) {
    return new SingleValueConditionDto(ConditionType.NEQ, value);
  }

  public static ConditionDto in(Object... values) {
    return new SingleValueConditionDto(ConditionType.IN, values);
  }

  public static ConditionDto lt(Object value) {
    return new SingleValueConditionDto(ConditionType.LT, value);
  }

  public static ConditionDto le(Object value) {
    return new SingleValueConditionDto(ConditionType.LE, value);
  }

  public static ConditionDto gt(Object value) {
    return new SingleValueConditionDto(ConditionType.GT, value);
  }

  public static ConditionDto ge(Object value) {
    return new SingleValueConditionDto(ConditionType.GE, value);
  }
}
