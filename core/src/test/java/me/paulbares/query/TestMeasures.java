package me.paulbares.query;

import me.paulbares.query.QueryExecutor.QueryScope;
import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.dto.TableDto;
import me.paulbares.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static me.paulbares.query.QueryBuilder.*;

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
    Measure agg3 = new AggregatedMeasure("qs", "quantity", "sum", "category", QueryBuilder.eq("drink"));
    Measure plus = new BinaryOperationMeasure("plus", BinaryOperator.PLUS, agg1, agg2);
    Measure divide = new BinaryOperationMeasure("divide", BinaryOperator.DIVIDE, agg1, plus);
    Measure constant = new DoubleConstantMeasure(100d);

    Assertions.assertThat(MeasureUtils.createExpression(agg1)).isEqualTo("sum(price)");
    Assertions.assertThat(MeasureUtils.createExpression(agg2)).isEqualTo("sum(quantity)");
    Assertions.assertThat(MeasureUtils.createExpression(agg3)).isEqualTo("sumIf(quantity, `category` = 'drink')");
    Assertions.assertThat(MeasureUtils.createExpression(plus)).isEqualTo("ps + qs");
    Assertions.assertThat(MeasureUtils.createExpression(divide)).isEqualTo("ps / plus");
    Assertions.assertThat(MeasureUtils.createExpression(constant)).isEqualTo("100.0");
    plus = new BinaryOperationMeasure("plus", BinaryOperator.PLUS, agg1, agg2);
    divide = new BinaryOperationMeasure("divide", BinaryOperator.DIVIDE, agg1, plus);
    Assertions.assertThat(MeasureUtils.createExpression(divide)).isEqualTo("ps / plus");

    AggregatedMeasure amount = new AggregatedMeasure("sum(Amount)", "Amount", AggregationFunction.SUM);
    AggregatedMeasure sales = new AggregatedMeasure("sales", "Amount", AggregationFunction.SUM, "Income/Expense", QueryBuilder.eq("Revenue"));
    Measure ebidtaRatio = QueryBuilder.divide("EBITDA %", amount, sales);

    ComparisonMeasureReferencePosition growth = periodComparison(
            "Growth",
            ComparisonMethod.DIVIDE,
            sales,
            Map.of("Year", "y-1"));
    Measure kpi = plus("KPI", ebidtaRatio, growth);

    Map<String, String> referencePosition = new LinkedHashMap<>();
    referencePosition.put("scenario encrypted", "s-1");
    referencePosition.put("group", "g");
    ComparisonMeasureReferencePosition kpiComp = bucketComparison(
            "KPI comp. with prev. scenario",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            kpi,
            referencePosition);

    Assertions.assertThat(MeasureUtils.createExpression(amount)).isEqualTo("sum(Amount)");
    Assertions.assertThat(MeasureUtils.createExpression(sales)).isEqualTo("sumIf(Amount, `Income/Expense` = 'Revenue')");
    Assertions.assertThat(MeasureUtils.createExpression(ebidtaRatio)).isEqualTo("sum(Amount) / sales");
    Assertions.assertThat(MeasureUtils.createExpression(growth)).isEqualTo("sales(current period) / sales(reference period), reference = {Year=y-1}");
    Assertions.assertThat(MeasureUtils.createExpression(kpi)).isEqualTo("EBITDA % + Growth");
    Assertions.assertThat(MeasureUtils.createExpression(kpiComp)).isEqualTo("KPI(current bucket) - KPI(reference bucket), reference = {scenario encrypted=s-1, group=g}");
  }

  @Test
  void testParentComparisonQueryScope() {
    Field continent = new Field("continent", String.class);
    Field country = new Field("country", String.class);
    Field city = new Field("city", String.class);
    Field other = new Field("other", String.class);

    Map<String, List<Field>> collect = List.of(continent, country, city, other).stream().collect(Collectors.groupingBy(Field::name));
    Function<String, Field> fieldSupplier = name -> collect.get(name).iterator().next();

    BiFunction<List<Field>, List<Field>, QueryScope> parentScopeProvider = (queryScopeColumns, pcmAncestors) -> {
      QueryScope queryScope = new QueryScope(new TableDto("myTable"), null, queryScopeColumns, Collections.emptyMap());
      return MeasureUtils.getParentScope(queryScope, QueryBuilder.parentComparison("pcm", ComparisonMethod.DIVIDE, QueryBuilder.sum("sum", "whatever"), pcmAncestors.stream().map(Field::name).toList()), fieldSupplier);
    };

    {
      Set<Field> queryFields = Set.of(continent, country, city);
      List<Field> ancestors = List.of(city, country, continent);
      MeasureUtils.checkQueryScopeForParentComparison(queryFields, ancestors);
      QueryScope parentScope = parentScopeProvider.apply(queryFields.stream().toList(), ancestors);
      Assertions.assertThat(parentScope.columns()).containsExactlyInAnyOrder(continent, country);
    }
    {
      Set<Field> queryFields = Set.of(continent, country, city);
      List<Field> ancestors = List.of(city, country);
      MeasureUtils.checkQueryScopeForParentComparison(queryFields, ancestors);
      QueryScope parentScope = parentScopeProvider.apply(queryFields.stream().toList(), ancestors);
      Assertions.assertThat(parentScope.columns()).containsExactlyInAnyOrder(continent, country);
    }
    {
      Set<Field> queryFields = Set.of(continent, country);
      List<Field> ancestors = List.of(city, country, continent);
      MeasureUtils.checkQueryScopeForParentComparison(queryFields, ancestors);
      QueryScope parentScope = parentScopeProvider.apply(queryFields.stream().toList(), ancestors);
      Assertions.assertThat(parentScope.columns()).containsExactlyInAnyOrder(continent);
    }
    {
      Set<Field> queryFields = Set.of(continent, country, city);
      List<Field> ancestors = List.of(city, continent);
      MeasureUtils.checkQueryScopeForParentComparison(queryFields, ancestors);
      QueryScope parentScope = parentScopeProvider.apply(queryFields.stream().toList(), ancestors);
      Assertions.assertThat(parentScope.columns()).containsExactlyInAnyOrder(continent, country);
    }
    {
      Set<Field> queryFields = Set.of(continent, country, city);
      List<Field> ancestors = List.of(other, country, continent);
      MeasureUtils.checkQueryScopeForParentComparison(queryFields, ancestors);
      QueryScope parentScope = parentScopeProvider.apply(queryFields.stream().toList(), ancestors);
      Assertions.assertThat(parentScope.columns()).containsExactlyInAnyOrder(continent, city);
    }
    {
      Set<Field> queryFields = Set.of(continent, country, city);
      List<Field> ancestors = List.of(country, continent);
      MeasureUtils.checkQueryScopeForParentComparison(queryFields, ancestors);
      QueryScope parentScope = parentScopeProvider.apply(queryFields.stream().toList(), ancestors);
      Assertions.assertThat(parentScope.columns()).containsExactlyInAnyOrder(continent, city);
    }
    {
      Set<Field> queryFields = Set.of(continent, country, other);
      List<Field> ancestors = List.of(country, continent);
      MeasureUtils.checkQueryScopeForParentComparison(queryFields, ancestors); // Ok
      QueryScope parentScope = parentScopeProvider.apply(queryFields.stream().toList(), ancestors);
      Assertions.assertThat(parentScope.columns()).containsExactlyInAnyOrder(other, continent);
    }
    {
      Set<Field> queryFields = Set.of(continent, city);
      List<Field> ancestors = List.of(city, country, continent);
      Assertions.assertThatThrownBy(() -> MeasureUtils.checkQueryScopeForParentComparison(queryFields, ancestors))
              .isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining(country + " field is used in a parent comparison. It should be set as column in the query.");
    }
  }
}
