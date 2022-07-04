package me.paulbares.query;

import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.context.Totals;
import me.paulbares.query.dto.*;

import java.util.List;
import java.util.Map;

import static me.paulbares.query.BinaryOperator.DIVIDE;
import static me.paulbares.query.BinaryOperator.PLUS;
import static me.paulbares.query.context.Totals.POSITION_BOTTOM;
import static me.paulbares.query.context.Totals.POSITION_TOP;
import static me.paulbares.query.dto.ConditionType.AND;
import static me.paulbares.query.dto.ConditionType.OR;

public class QueryBuilder {

  public static final Totals TOP = new Totals(POSITION_TOP);
  public static final Totals BOTTOM = new Totals(POSITION_BOTTOM);

  public static QueryDto query() {
    return new QueryDto();
  }

  public static AggregatedMeasure aggregatedMeasure(String field, String aggregationFunction) {
    return new AggregatedMeasure(field, aggregationFunction);
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

  // FIXME new

  public static void addPeriodColumnSet(QueryDto query, Period period) {
    query.withColumnSet(QueryDto.PERIOD, new PeriodColumnSetDto(period));
  }

  public static void addBucketColumnSet(QueryDto query, String name, String field, Map<String, List<String>> values) {
    BucketColumnSetDto columnSet = new BucketColumnSetDto(name, field);
    columnSet.values = values;
    query.withColumnSet(QueryDto.BUCKET, columnSet);
  }

  public static ComparisonMeasure periodComparison(String alias,
                                                   ComparisonMethod method,
                                                   Measure measure,
                                                   Map<String, String> referencePosition) {
    return new ComparisonMeasure(
            alias,
            method,
            measure,
            QueryDto.PERIOD,
            referencePosition);
  }

  public static ComparisonMeasure bucketComparison(String alias,
                                                   ComparisonMethod method,
                                                   Measure measure,
                                                   Map<String, String> referencePosition) {
    return new ComparisonMeasure(
            alias,
            method,
            measure,
            QueryDto.BUCKET,
            referencePosition);
  }

  public static Measure divide(String alias, Measure a, Measure b) {
    return new BinaryOperationMeasure(alias, DIVIDE ,a, b);
  }

  public static Measure plus(String alias, Measure a, Measure b) {
    return new BinaryOperationMeasure(alias, PLUS ,a, b);
  }

  public static void main(String[] args) {
    QueryDto query = QueryBuilder.query();

    query.table("saas");

    QueryBuilder.addBucketColumnSet(query,
            "group",
            "scenario encrypted",
            Map.of("group1", List.of("A", "B", "C", "D"), "group2", List.of("A", "D")));

    QueryBuilder.addPeriodColumnSet(query, new Period.Year("Year"));

    AggregatedMeasure amount = new AggregatedMeasure("Amount", AggregationFunction.SUM);
    AggregatedMeasure sales = new AggregatedMeasure("sales", "Amount", AggregationFunction.SUM, "Income/Expense", QueryBuilder.eq("Revenue"));
    query.withMeasure(amount);
    query.withMeasure(sales);
    Measure ebidtaRatio = QueryBuilder.divide("EBITDA %", amount, sales);
    query.withMeasure(ebidtaRatio);

    ComparisonMeasure growth = periodComparison(
            "Growth",
            ComparisonMethod.DIVIDE,
            sales,
            Map.of("Year", "y-1"));
    query.withMeasure(growth);
    Measure kpi = plus("KPI", ebidtaRatio, growth);
    query.withMeasure(kpi);

    ComparisonMeasure kpiComp = bucketComparison(
            "KPI comp. with prev. scenario",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            kpi,
            Map.of("scenario encrypted", "s-1", "group", "g"));
    query.withMeasure(kpiComp);

    System.out.println(query.json());
  }
}
