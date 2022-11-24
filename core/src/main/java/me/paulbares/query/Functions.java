package me.paulbares.query;

import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.database.SQLTranslator;
import me.paulbares.query.dto.*;
import me.paulbares.store.Field;

import java.util.List;

import static me.paulbares.query.BinaryOperator.*;
import static me.paulbares.query.dto.ConditionType.AND;
import static me.paulbares.query.dto.ConditionType.OR;

public class Functions {

  public static CriteriaDto criterion(String field, ConditionDto conditionDto) {
    return new CriteriaDto(field, conditionDto);
  }

  public static CriteriaDto all(CriteriaDto... criteria) {
    return buildCriterionDto(AND, criteria);
  }

  public static CriteriaDto any(CriteriaDto... criteria) {
    return buildCriterionDto(OR, criteria);
  }

  public static CriteriaDto buildCriterionDto(ConditionType conditionType, CriteriaDto... criteria) {
    return new CriteriaDto(conditionType, List.of(criteria));
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

  public static ConditionDto isNull() {
    return ConditionDto.NULL_CONDITION;
  }

  public static ConditionDto isNotNull() {
    return ConditionDto.NOT_NULL_CONDITION;
  }

  public static ConditionDto neq(Object value) {
    return new SingleValueConditionDto(ConditionType.NEQ, value);
  }

  public static ConditionDto in(Object... values) {
    return new InConditionDto(values);
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

  public static ConditionDto like(String value) {
    return new SingleValueConditionDto(ConditionType.LIKE, value);
  }

  public static Measure divide(String alias, Measure a, Measure b) {
    return new BinaryOperationMeasure(alias, DIVIDE, a, b);
  }

  public static Measure multiply(String alias, Measure a, Measure b) {
    return new BinaryOperationMeasure(alias, MULTIPLY, a, b);
  }

  public static Measure minus(String alias, Measure a, Measure b) {
    return new BinaryOperationMeasure(alias, MINUS, a, b);
  }

  public static Measure plus(String alias, Measure a, Measure b) {
    return new BinaryOperationMeasure(alias, PLUS, a, b);
  }

  public static Measure min(String alias, String field) {
    return new AggregatedMeasure(alias, field, AggregationFunction.MIN);
  }

  public static Measure sum(String alias, String field) {
    return new AggregatedMeasure(alias, field, AggregationFunction.SUM);
  }

  public static Measure sumIf(String alias, String field, CriteriaDto criteriaDto) {
    return new AggregatedMeasure(alias, field, AggregationFunction.SUM, criteriaDto);
  }

  public static Measure avg(String alias, String field) {
    return new AggregatedMeasure(alias, field, AggregationFunction.AVG);
  }

  public static Measure integer(long value) {
    return new LongConstantMeasure(value);
  }

  public static Measure decimal(double value) {
    return new DoubleConstantMeasure(value);
  }

  public static void main(String[] args) {
    // FIXME
    eq("A").and(eq("B"));
    CriteriaDto c1 = criterion("f1", eq("A"));
    CriteriaDto c2 = criterion("f1", neq("B"));
    CriteriaDto c3 = criterion("f2", isNotNull());
    CriteriaDto c1Andc2 = all(c1, c2);
    CriteriaDto any = any(c1Andc2, c3);// (f1 = A AND f1<>B) OR f2 <> NULL
    System.out.println(SQLTranslator.toSql(s -> new Field(s, String.class), any));
  }
}
