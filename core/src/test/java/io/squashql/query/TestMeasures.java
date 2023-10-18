package io.squashql.query;

import io.squashql.query.agg.AggregationFunction;
import io.squashql.query.dto.Period;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.squashql.query.Functions.*;
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;

public class TestMeasures {

  @Test
  void testAggregatedMeasureConstructor() {
    Assertions.assertThatThrownBy(() -> new AggregatedMeasure("null", null, AggregationFunction.SUM))
            .isInstanceOf(NullPointerException.class);
    Assertions.assertThatThrownBy(() -> new AggregatedMeasure("null", "field", null))
            .isInstanceOf(NullPointerException.class);
    Assertions.assertThatThrownBy(() -> new AggregatedMeasure(null, "field", AggregationFunction.SUM))
            .isInstanceOf(NullPointerException.class);
  }

  @Test
  void testExpressions() {
    Measure agg1 = new AggregatedMeasure("ps", "price", "sum");
    Measure agg2 = new AggregatedMeasure("qs", "quantity", "sum");
    Measure agg3 = new AggregatedMeasure("qs", "quantity", "sum", criterion("category", eq(new ConstantField("drink"))));
    Measure plus = new BinaryOperationMeasure("plus", BinaryOperator.PLUS, agg1, agg2);
    Measure divide = new BinaryOperationMeasure("divide", BinaryOperator.DIVIDE, agg1, plus);
    Measure constant = new DoubleConstantMeasure(100d);

    Assertions.assertThat(MeasureUtils.createExpression(agg1)).isEqualTo("sum(price)");
    Assertions.assertThat(MeasureUtils.createExpression(agg2)).isEqualTo("sum(quantity)");
    Assertions.assertThat(MeasureUtils.createExpression(agg3)).isEqualTo("sumIf(quantity, category = 'drink')");
    Assertions.assertThat(MeasureUtils.createExpression(plus)).isEqualTo("ps + qs");
    Assertions.assertThat(MeasureUtils.createExpression(divide)).isEqualTo("ps / plus");
    Assertions.assertThat(MeasureUtils.createExpression(constant)).isEqualTo("100.0");
    plus = new BinaryOperationMeasure("plus", BinaryOperator.PLUS, agg1, agg2);
    divide = new BinaryOperationMeasure("divide", BinaryOperator.DIVIDE, agg1, plus);
    Assertions.assertThat(MeasureUtils.createExpression(divide)).isEqualTo("ps / plus");

    AggregatedMeasure amount = new AggregatedMeasure("sum(Amount)", "Amount", AggregationFunction.SUM);
    AggregatedMeasure sales = new AggregatedMeasure("sales", "Amount", AggregationFunction.SUM, criterion("Income/Expense", eq(new ConstantField("Revenue"))));
    Measure ebidtaRatio = Functions.divide("EBITDA %", amount, sales);
    final Period.Year period = new Period.Year(tableField("Year"));
    ComparisonMeasureReferencePosition growth = new ComparisonMeasureReferencePosition(
            "Growth",
            ComparisonMethod.DIVIDE,
            sales,
            Map.of(period.year(), "y-1"),
            period);
    Measure kpi = plus("KPI", ebidtaRatio, growth);

    Map<Field, String> referencePosition = new LinkedHashMap<>();
    referencePosition.put(tableField("scenario encrypted"), "s-1");
    referencePosition.put(tableField("group"), "g");
    ComparisonMeasureReferencePosition kpiComp = new ComparisonMeasureReferencePosition(
            "KPI comp. with prev. scenario",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            kpi,
            referencePosition,
            ColumnSetKey.BUCKET);

    ComparisonMeasureReferencePosition parentComparisonMeasure = new ComparisonMeasureReferencePosition("parent", ComparisonMethod.DIVIDE, amount, tableFields(List.of("city", "country", "continent")));

    Assertions.assertThat(MeasureUtils.createExpression(amount)).isEqualTo("sum(Amount)");
    Assertions.assertThat(MeasureUtils.createExpression(sales)).isEqualTo("sumIf(Amount, Income/Expense = 'Revenue')");
    Assertions.assertThat(MeasureUtils.createExpression(ebidtaRatio)).isEqualTo("sum(Amount) / sales");
    Assertions.assertThat(MeasureUtils.createExpression(growth)).isEqualTo("sales(current) / sales(reference), reference = [Year=y-1]");
    Assertions.assertThat(MeasureUtils.createExpression(kpi)).isEqualTo("EBITDA % + Growth");
    Assertions.assertThat(MeasureUtils.createExpression(kpiComp)).isEqualTo("KPI(current) - KPI(reference), reference = [scenario encrypted=s-1, group=g]");
    Assertions.assertThat(MeasureUtils.createExpression(parentComparisonMeasure)).isEqualTo("sum(Amount) / sum(Amount)(parent), ancestors = [city, country, continent]");
  }
}
