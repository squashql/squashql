package me.paulbares.query;

import me.paulbares.query.agg.AggregationFunction;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static me.paulbares.query.QueryBuilder.*;

public class TestMeasureExpression {

  @Test
  void test() {
    Measure agg1 = new AggregatedMeasure("price", "sum");
    Measure agg2 = new AggregatedMeasure("quantity", "sum");
    Measure agg3 = new AggregatedMeasure("quantity", "sum", "category", QueryBuilder.eq("drink"));
    Measure plus = new BinaryOperationMeasure(null, BinaryOperator.PLUS, agg1, agg2);
    Measure divide = new BinaryOperationMeasure(null, BinaryOperator.DIVIDE, agg1, plus);

    Assertions.assertThat(agg1.expression()).isEqualTo("sum(price)");
    Assertions.assertThat(agg2.expression()).isEqualTo("sum(quantity)");
    Assertions.assertThat(agg3.expression()).isEqualTo("sumIf(quantity, `category` = 'drink')");
    Assertions.assertThat(plus.expression()).isEqualTo("sum(price) + sum(quantity)");
    Assertions.assertThat(divide.expression()).isEqualTo("sum(price) / (sum(price) + sum(quantity))");
    plus = new BinaryOperationMeasure("plus", BinaryOperator.PLUS, agg1, agg2);
    divide = new BinaryOperationMeasure(null, BinaryOperator.DIVIDE, agg1, plus);
    Assertions.assertThat(divide.expression()).isEqualTo("sum(price) / plus");

    AggregatedMeasure amount = new AggregatedMeasure("Amount", AggregationFunction.SUM);
    AggregatedMeasure sales = new AggregatedMeasure("sales", "Amount", AggregationFunction.SUM, "Income/Expense", QueryBuilder.eq("Revenue"));
    Measure ebidtaRatio = QueryBuilder.divide("EBITDA %", amount, sales);

    ComparisonMeasure growth = periodComparison(
            "Growth",
            ComparisonMethod.DIVIDE,
            sales,
            Map.of("Year", "y-1"));
    Measure kpi = plus("KPI", ebidtaRatio, growth);

    Map<String, String> referencePosition = new LinkedHashMap<>();
    referencePosition.put("scenario encrypted", "s-1");
    referencePosition.put("group", "g");
    ComparisonMeasure kpiComp = bucketComparison(
            "KPI comp. with prev. scenario",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            kpi,
            referencePosition);

    Assertions.assertThat(amount.expression()).isEqualTo("sum(Amount)");
    Assertions.assertThat(sales.expression()).isEqualTo("sumIf(Amount, `Income/Expense` = 'Revenue')");
    Assertions.assertThat(ebidtaRatio.expression()).isEqualTo("sum(Amount) / sales");
    Assertions.assertThat(growth.expression()).isEqualTo("sales(current period) / sales(reference period), reference = {Year=y-1}");
    Assertions.assertThat(kpi.expression()).isEqualTo("EBITDA % + Growth");
    Assertions.assertThat(kpiComp.expression()).isEqualTo("KPI(current bucket) - KPI(reference bucket), reference = {scenario encrypted=s-1, group=g}");
  }
}
