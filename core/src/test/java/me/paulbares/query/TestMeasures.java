package me.paulbares.query;

import me.paulbares.query.QueryExecutor.QueryScope;
import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.TableDto;
import me.paulbares.store.TypedField;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.TriFunction;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static me.paulbares.query.Functions.eq;
import static me.paulbares.query.Functions.plus;

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
    Measure agg3 = new AggregatedMeasure("qs", "quantity", "sum", "category", eq("drink"));
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
    AggregatedMeasure sales = new AggregatedMeasure("sales", "Amount", AggregationFunction.SUM, "Income/Expense", eq("Revenue"));
    Measure ebidtaRatio = Functions.divide("EBITDA %", amount, sales);

    ComparisonMeasureReferencePosition growth = new ComparisonMeasureReferencePosition(
            "Growth",
            ComparisonMethod.DIVIDE,
            sales,
            ColumnSetKey.PERIOD,
            Map.of("Year", "y-1"));
    Measure kpi = plus("KPI", ebidtaRatio, growth);

    Map<String, String> referencePosition = new LinkedHashMap<>();
    referencePosition.put("scenario encrypted", "s-1");
    referencePosition.put("group", "g");
    ComparisonMeasureReferencePosition kpiComp = new ComparisonMeasureReferencePosition(
            "KPI comp. with prev. scenario",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            kpi,
            ColumnSetKey.BUCKET,
            referencePosition);

    ParentComparisonMeasure parentComparisonMeasure = new ParentComparisonMeasure("parent", ComparisonMethod.DIVIDE, amount, List.of("city", "country", "continent"));

    Assertions.assertThat(MeasureUtils.createExpression(amount)).isEqualTo("sum(Amount)");
    Assertions.assertThat(MeasureUtils.createExpression(sales)).isEqualTo("sumIf(Amount, `Income/Expense` = 'Revenue')");
    Assertions.assertThat(MeasureUtils.createExpression(ebidtaRatio)).isEqualTo("sum(Amount) / sales");
    Assertions.assertThat(MeasureUtils.createExpression(growth)).isEqualTo("sales(current period) / sales(reference period), reference = {Year=y-1}");
    Assertions.assertThat(MeasureUtils.createExpression(kpi)).isEqualTo("EBITDA % + Growth");
    Assertions.assertThat(MeasureUtils.createExpression(kpiComp)).isEqualTo("KPI(current bucket) - KPI(reference bucket), reference = {scenario encrypted=s-1, group=g}");
    Assertions.assertThat(MeasureUtils.createExpression(parentComparisonMeasure)).isEqualTo("sum(Amount) / sum(Amount)(parent), ancestors = [city, country, continent]");
  }

  @Test
  void testParentComparisonQueryScope() {
    TypedField continent = new TypedField("continent", String.class);
    TypedField country = new TypedField("country", String.class);
    TypedField city = new TypedField("city", String.class);
    TypedField other = new TypedField("other", String.class);

    Map<String, List<TypedField>> collect = List.of(continent, country, city, other).stream().collect(Collectors.groupingBy(TypedField::name));
    Function<String, TypedField> fieldSupplier = name -> collect.get(name).iterator().next();

    BiFunction<List<TypedField>, List<TypedField>, QueryScope> parentScopeProvider = (queryScopeColumns, pcmAncestors) -> {
      QueryScope queryScope = new QueryScope(new TableDto("myTable"), null, queryScopeColumns, Collections.emptyMap());
      return MeasureUtils.getParentScopeWithClearedConditions(queryScope, new ParentComparisonMeasure("pcm", ComparisonMethod.DIVIDE, Functions.sum("sum", "whatever"), pcmAncestors.stream().map(TypedField::name).toList()), fieldSupplier);
    };

    {
      Set<TypedField> queryFields = Set.of(continent, country, city);
      List<TypedField> ancestors = List.of(city, country, continent);
      QueryScope parentScope = parentScopeProvider.apply(queryFields.stream().toList(), ancestors);
      Assertions.assertThat(parentScope.columns()).containsExactlyInAnyOrder(continent, country);
    }
    {
      Set<TypedField> queryFields = Set.of(continent, country, city);
      List<TypedField> ancestors = List.of(city, country);
      QueryScope parentScope = parentScopeProvider.apply(queryFields.stream().toList(), ancestors);
      Assertions.assertThat(parentScope.columns()).containsExactlyInAnyOrder(continent, country);
    }
    {
      Set<TypedField> queryFields = Set.of(continent, country);
      List<TypedField> ancestors = List.of(city, country, continent);
      QueryScope parentScope = parentScopeProvider.apply(queryFields.stream().toList(), ancestors);
      Assertions.assertThat(parentScope.columns()).containsExactlyInAnyOrder(continent);
    }
    {
      Set<TypedField> queryFields = Set.of(continent, country, city);
      List<TypedField> ancestors = List.of(city, continent);
      QueryScope parentScope = parentScopeProvider.apply(queryFields.stream().toList(), ancestors);
      Assertions.assertThat(parentScope.columns()).containsExactlyInAnyOrder(continent, country);
    }
    {
      Set<TypedField> queryFields = Set.of(continent, country, city);
      List<TypedField> ancestors = List.of(other, country, continent);
      QueryScope parentScope = parentScopeProvider.apply(queryFields.stream().toList(), ancestors);
      Assertions.assertThat(parentScope.columns()).containsExactlyInAnyOrder(continent, city);
    }
    {
      Set<TypedField> queryFields = Set.of(continent, country, city);
      List<TypedField> ancestors = List.of(country, continent);
      QueryScope parentScope = parentScopeProvider.apply(queryFields.stream().toList(), ancestors);
      Assertions.assertThat(parentScope.columns()).containsExactlyInAnyOrder(continent, city);
    }
    {
      Set<TypedField> queryFields = Set.of(continent, country, other);
      List<TypedField> ancestors = List.of(country, continent);
      QueryScope parentScope = parentScopeProvider.apply(queryFields.stream().toList(), ancestors);
      Assertions.assertThat(parentScope.columns()).containsExactlyInAnyOrder(other, continent);
    }
  }

  @Test
  void testParentComparisonQueryScopeWithCondition() {
    TypedField continent = new TypedField("continent", String.class);
    TypedField country = new TypedField("country", String.class);
    TypedField city = new TypedField("city", String.class);
    TypedField other = new TypedField("other", String.class);

    Map<String, List<TypedField>> collect = List.of(continent, country, city, other).stream().collect(Collectors.groupingBy(TypedField::name));
    Function<String, TypedField> fieldSupplier = name -> collect.get(name).iterator().next();

    TriFunction<List<TypedField>, List<TypedField>, Map<String, ConditionDto>, QueryScope> parentScopeProvider = (queryScopeColumns, pcmAncestors, conditions) -> {
      QueryScope queryScope = new QueryScope(new TableDto("myTable"),
              null,
              queryScopeColumns,
              conditions);
      return MeasureUtils.getParentScopeWithClearedConditions(queryScope, new ParentComparisonMeasure("pcm", ComparisonMethod.DIVIDE, Functions.sum("sum", "whatever"), pcmAncestors.stream().map(TypedField::name).toList()), fieldSupplier);
    };

    {
      Set<TypedField> queryFields = Set.of(continent, country, city);
      List<TypedField> ancestors = List.of(city, country, continent);
      QueryScope parentScope = parentScopeProvider.apply(queryFields.stream().toList(), ancestors, Map.of("city", eq("paris"), "other", eq("value")));
      Assertions.assertThat(parentScope.columns()).containsExactlyInAnyOrder(continent, country);
      Assertions.assertThat(parentScope.conditions()).isEqualTo(Map.of("other", eq("value")));
    }
  }
}
