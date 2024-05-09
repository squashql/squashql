package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.GroupColumnSetDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.squashql.query.ComparisonMethod.DIVIDE;
import static io.squashql.query.ComparisonMethod.RELATIVE_DIFFERENCE;
import static io.squashql.query.Functions.*;
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;
import static io.squashql.transaction.DataLoader.MAIN_SCENARIO_NAME;
import static io.squashql.transaction.DataLoader.SCENARIO_FIELD_NAME;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestGroupComparison extends ABaseTestQuery {

  private static final String groupOfScenario = "Group of scenario";
  private final String storeName = "store" + getClass().getSimpleName().toLowerCase();
  private final TableField ean = new TableField(this.storeName, "ean");
  private final TableField category = new TableField(this.storeName, "category");
  private final TableField price = new TableField(this.storeName, "price");
  private final TableField qty = new TableField(this.storeName, "quantity");
  private final TableField scenario = new TableField(this.storeName, SCENARIO_FIELD_NAME);

  protected GroupColumnSetDto groupCS = new GroupColumnSetDto(groupOfScenario, tableField(SCENARIO_FIELD_NAME))
          .withNewGroup("group1", List.of(MAIN_SCENARIO_NAME, "s1"))
          .withNewGroup("group2", List.of(MAIN_SCENARIO_NAME, "s2"))
          .withNewGroup("group3", List.of(MAIN_SCENARIO_NAME, "s1", "s2"));

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField scenario = new TableTypedField(this.storeName, "scenario", String.class);
    TableTypedField ean = new TableTypedField(this.storeName, "ean", String.class);
    TableTypedField category = new TableTypedField(this.storeName, "category", String.class);
    TableTypedField price = new TableTypedField(this.storeName, "price", double.class);
    TableTypedField qty = new TableTypedField(this.storeName, "quantity", int.class);
    return Map.of(this.storeName, List.of(scenario, ean, category, price, qty));
  }

  @Override
  protected void loadData() {
    this.tm.load(this.storeName, List.of(
            new Object[]{MAIN_SCENARIO_NAME, "bottle", "drink", 2d, 11},
            new Object[]{MAIN_SCENARIO_NAME, "cookie", "food", 3d, 20},
            new Object[]{MAIN_SCENARIO_NAME, "shirt", "cloth", 10d, 3}
    ));

    this.tm.load(this.storeName, List.of(
            new Object[]{"s1", "bottle", "drink", 4d, 9},
            new Object[]{"s1", "cookie", "food", 4d, 20},
            new Object[]{"s1", "shirt", "cloth", 11d, 3}
    ));

    this.tm.load(this.storeName, List.of(
            new Object[]{"s2", "bottle", "drink", 1.5d, 12},
            new Object[]{"s2", "cookie", "food", 2.5d, 20},
            new Object[]{"s2", "shirt", "cloth", 9d, 3}
    ));
  }

  @Test
  void testAbsoluteDifferenceWithFirst() {
    GroupColumnSetDto group = new GroupColumnSetDto(groupOfScenario, this.scenario)
            .withNewGroup("group1", List.of(MAIN_SCENARIO_NAME, "s1"))
            .withNewGroup("group2", List.of(MAIN_SCENARIO_NAME, "s2"))
            .withNewGroup("group3", List.of(MAIN_SCENARIO_NAME, "s1", "s2"));

    AggregatedMeasure price = new AggregatedMeasure("p", "price", "sum");
    ComparisonMeasureReferencePosition priceComp = new ComparisonMeasureReferencePosition(
            "priceDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            price,
            Map.of(
                    this.scenario, AComparisonExecutor.REF_POS_FIRST,
                    tableField(groupOfScenario), "g"
            ),
            ColumnSetKey.GROUP);
    AggregatedMeasure quantity = new AggregatedMeasure("q", "quantity", "sum");
    ComparisonMeasureReferencePosition quantityComp = new ComparisonMeasureReferencePosition(
            "quantityDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            quantity,
            Map.of(
                    this.scenario, AComparisonExecutor.REF_POS_FIRST,
                    tableField(groupOfScenario), "g"
            ),
            ColumnSetKey.GROUP);

    var query = Query
            .from(this.storeName)
            .select_(List.of(group), List.of(priceComp, price, quantityComp, quantity))
            .build();

    Table dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset.headers().stream().map(Header::name)).containsExactly(
            groupOfScenario, SqlUtils.squashqlExpression(this.scenario),
            "priceDiff", "p",
            "quantityDiff", "q");
    Assertions.assertThat(dataset).containsExactly(
            List.of("group1", MAIN_SCENARIO_NAME, 0d, 15d, 0l, 34l),
            List.of("group1", "s1", 4d, 19d, -2l, 32l),
            List.of("group2", MAIN_SCENARIO_NAME, 0d, 15d, 0l, 34l),
            List.of("group2", "s2", -2d, 13d, 1l, 35l),
            List.of("group3", MAIN_SCENARIO_NAME, 0d, 15d, 0l, 34l),
            List.of("group3", "s1", 4d, 19d, -2l, 32l),
            List.of("group3", "s2", -2d, 13d, 1l, 35l));

    // Add a condition
    query = Query
            .from(this.storeName)
            .where(this.scenario, eq("s1"))
            .select_(List.of(group), List.of(priceComp))
            .build();

    dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset).containsExactly(
            List.of("group1", "s1", 4d),
            List.of("group3", "s1", 4d));
  }

  @Test
  void testAbsoluteDifferenceWithPrevious() {
    AggregatedMeasure price = new AggregatedMeasure("p", "price", "sum");
    ComparisonMeasureReferencePosition priceComp = new ComparisonMeasureReferencePosition(
            "priceDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            price,
            Map.of(
                    tableField(SCENARIO_FIELD_NAME), "s-1",
                    tableField(groupOfScenario), "g"
            ),
            ColumnSetKey.GROUP);
    AggregatedMeasure quantity = new AggregatedMeasure("q", "quantity", "sum");
    ComparisonMeasureReferencePosition quantityComp = new ComparisonMeasureReferencePosition(
            "quantityDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            quantity,
            Map.of(tableField(SCENARIO_FIELD_NAME), "s-1", tableField(groupOfScenario), "g"),
            ColumnSetKey.GROUP);

    var query = Query.from(this.storeName)
            .select(List.of(), List.of(this.groupCS), List.of(priceComp, price, quantityComp, quantity))
            .build();

    Table dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset.headers().stream().map(Header::name)).containsExactly(
            groupOfScenario, SCENARIO_FIELD_NAME,
            "priceDiff", "p",
            "quantityDiff", "q");
    Assertions.assertThat(dataset).containsExactly(
            List.of("group1", MAIN_SCENARIO_NAME, 0d, 15d, 0l, 34l),
            List.of("group1", "s1", 4d, 19d, -2l, 32l),
            List.of("group2", MAIN_SCENARIO_NAME, 0d, 15d, 0l, 34l),
            List.of("group2", "s2", -2d, 13d, 1l, 35l),
            List.of("group3", MAIN_SCENARIO_NAME, 0d, 15d, 0l, 34l),
            List.of("group3", "s1", 4d, 19d, -2l, 32l),
            List.of("group3", "s2", -6d, 13d, 3l, 35l));
  }

  @Test
  void testRelativeDifferenceWithFirst() {
    AggregatedMeasure price = new AggregatedMeasure("p", "price", "sum");
    ComparisonMeasureReferencePosition priceComp = new ComparisonMeasureReferencePosition(
            "priceDiff",
            RELATIVE_DIFFERENCE,
            price,
            Map.of(
                    tableField(SCENARIO_FIELD_NAME), AComparisonExecutor.REF_POS_FIRST,
                    tableField(groupOfScenario), "g"
            ),
            ColumnSetKey.GROUP);
    AggregatedMeasure quantity = new AggregatedMeasure("q", "quantity", "sum");
    ComparisonMeasureReferencePosition quantityComp = new ComparisonMeasureReferencePosition(
            "quantityDiff",
            RELATIVE_DIFFERENCE,
            quantity,
            Map.of(
                    tableField(SCENARIO_FIELD_NAME), AComparisonExecutor.REF_POS_FIRST,
                    tableField(groupOfScenario), "g"
            ),
            ColumnSetKey.GROUP);

    var query = new QueryDto()
            .table(this.storeName)
            .withColumnSet(ColumnSetKey.GROUP, this.groupCS)
            .withMeasure(priceComp)
            .withMeasure(price)
            .withMeasure(quantityComp)
            .withMeasure(quantity);

    Table dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset.headers().stream().map(Header::name)).containsExactly(
            groupOfScenario, SCENARIO_FIELD_NAME,
            "priceDiff", "p",
            "quantityDiff", "q");
    Assertions.assertThat(dataset).containsExactly(
            List.of("group1", MAIN_SCENARIO_NAME, 0d, 15d, 0d, 34l),
            List.of("group1", "s1", 0.26666666666666666d, 19d, -0.058823529411764705d, 32l),
            List.of("group2", MAIN_SCENARIO_NAME, 0d, 15d, 0d, 34l),
            List.of("group2", "s2", -0.13333333333333333d, 13d, 0.029411764705882353d, 35l),
            List.of("group3", MAIN_SCENARIO_NAME, 0d, 15d, 0d, 34l),
            List.of("group3", "s1", 0.26666666666666666d, 19d, -0.058823529411764705d, 32l),
            List.of("group3", "s2", -0.13333333333333333d, 13d, 0.029411764705882353d, 35l));
  }

  @Test
  void testOrderIsPreserved() {
    // The following order should be respected even if columns are ordered by default.
    GroupColumnSetDto groupCS = new GroupColumnSetDto(groupOfScenario, tableField(SCENARIO_FIELD_NAME))
            .withNewGroup("B", List.of("s1", MAIN_SCENARIO_NAME))
            .withNewGroup("A", List.of("s2", MAIN_SCENARIO_NAME, "s1"))
            .withNewGroup("C", List.of(MAIN_SCENARIO_NAME, "s2", "s1"));

    var query = Query
            .from(this.storeName)
            .select_(List.of(groupCS), List.of(CountMeasure.INSTANCE))
            .build();

    Table dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset.headers().stream().map(Header::name))
            .containsExactly(groupOfScenario, SCENARIO_FIELD_NAME, CountMeasure.ALIAS);
    Assertions.assertThat(dataset).containsExactly(
            List.of("B", "s1", 3l),
            List.of("B", MAIN_SCENARIO_NAME, 3l),
            List.of("A", "s2", 3l),
            List.of("A", MAIN_SCENARIO_NAME, 3l),
            List.of("A", "s1", 3l),
            List.of("C", MAIN_SCENARIO_NAME, 3l),
            List.of("C", "s2", 3l),
            List.of("C", "s1", 3l));
  }

  @Test
  void testOrderIsPreservedAndNaturallyOrderOnOtherColumns() {
    // The following order should be respected even if columns are ordered by default.
    GroupColumnSetDto groupCS = new GroupColumnSetDto(groupOfScenario, tableField(SCENARIO_FIELD_NAME))
            .withNewGroup("B", List.of("s1", MAIN_SCENARIO_NAME))
            .withNewGroup("A", List.of("s2", MAIN_SCENARIO_NAME));

    // Add category in the query. The table should be ordered first according the implicit order of the groups and then
    // by category.
    var query = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(groupCS), List.of(CountMeasure.INSTANCE))
            .build();

    Table dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset).containsExactly(
            List.of("B", "s1", "cloth", 1l),
            List.of("B", "s1", "drink", 1l),
            List.of("B", "s1", "food", 1l),
            List.of("B", "base", "cloth", 1l),
            List.of("B", "base", "drink", 1l),
            List.of("B", "base", "food", 1l),

            List.of("A", "s2", "cloth", 1l),
            List.of("A", "s2", "drink", 1l),
            List.of("A", "s2", "food", 1l),
            List.of("A", "base", "cloth", 1l),
            List.of("A", "base", "drink", 1l),
            List.of("A", "base", "food", 1l));
  }

  @Test
  void testTotal() {
    // The following order should be respected even if columns are ordered by default.
    GroupColumnSetDto groupCS = new GroupColumnSetDto(groupOfScenario, tableField(SCENARIO_FIELD_NAME))
            .withNewGroup("B", List.of("s1", MAIN_SCENARIO_NAME))
            .withNewGroup("A", List.of("s2", MAIN_SCENARIO_NAME, "s1"))
            .withNewGroup("C", List.of(MAIN_SCENARIO_NAME, "s2", "s1"));

    var query = Query
            .from(this.storeName)
            .select_(List.of(groupCS), List.of(CountMeasure.INSTANCE))
            .rollup(tableFields(List.of(SCENARIO_FIELD_NAME))) // should not affect the comparison engine
            .build();

    Table dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset.headers().stream().map(Header::name))
            .containsExactly(groupOfScenario, SCENARIO_FIELD_NAME, CountMeasure.ALIAS);
    Assertions.assertThat(dataset).containsExactly(
            List.of("B", "s1", 3l),
            List.of("B", MAIN_SCENARIO_NAME, 3l),
            List.of("A", "s2", 3l),
            List.of("A", MAIN_SCENARIO_NAME, 3l),
            List.of("A", "s1", 3l),
            List.of("C", MAIN_SCENARIO_NAME, 3l),
            List.of("C", "s2", 3l),
            List.of("C", "s1", 3l));
  }

  @Test
  void testAbsoluteDifferenceWithPreviousSingleGroupUndefined() {
    Measure price = sum("p", this.price);
    ComparisonMeasureReferencePosition priceCompPrev = new ComparisonMeasureReferencePosition(
            "priceCompPrev",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            price,
            Map.of(this.scenario, "s-1"));

    ComparisonMeasureReferencePosition priceCompFirst = new ComparisonMeasureReferencePosition(
            "priceCompFirst",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            price,
            Map.of(this.scenario, "first"));

    var query = Query
            .from(this.storeName)
            .select(List.of(this.scenario), List.of(price, priceCompPrev, priceCompFirst))
            .build();

    Table dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset.headers().stream().map(Header::name)).containsExactly(
            SqlUtils.getFieldFullName(this.storeName, SCENARIO_FIELD_NAME),
            "p",
            "priceCompPrev",
            "priceCompFirst");
    Assertions.assertThat(dataset).containsExactly(
            List.of("base", 15d, 0d, 0d),
            List.of("s1", 19d, 4d, 4d),
            List.of("s2", 13d, -6d, -2d));

    // With a filter
    query = Query
            .from(this.storeName)
            .where(this.scenario, eq("s2"))
            .select(List.of(this.scenario), List.of(price, priceCompPrev, priceCompFirst))
            .build();

    dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset.headers().stream().map(Header::name)).containsExactly(
            SqlUtils.getFieldFullName(this.storeName, SCENARIO_FIELD_NAME),
            "p",
            "priceCompPrev",
            "priceCompFirst");
    Assertions.assertThat(dataset).containsExactly(List.of("s2", 13d, -6d, -2d));

    query = Query
            .from(this.storeName)
            .select(List.of(this.scenario, this.ean), List.of(price, priceCompPrev, priceCompFirst))
            .build();

    dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset).containsExactly(
            List.of(MAIN_SCENARIO_NAME, "bottle", 2d, 0d, 0d),
            List.of(MAIN_SCENARIO_NAME, "cookie", 3d, 0d, 0d),
            List.of(MAIN_SCENARIO_NAME, "shirt", 10d, 0d, 0d),
            List.of("s1", "bottle", 4d, 2d, 2d),
            List.of("s1", "cookie", 4d, 1d, 1d),
            List.of("s1", "shirt", 11d, 1d, 1d),
            List.of("s2", "bottle", 1.5d, -2.5d, -0.5d),
            List.of("s2", "cookie", 2.5d, -1.5d, -0.5d),
            List.of("s2", "shirt", 9d, -2d, -1d));

    query = Query
            .from(this.storeName)
            .select(List.of(this.ean, this.scenario), List.of(price, priceCompPrev, priceCompFirst))
            .build();

    dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset).containsExactly(
            List.of("bottle", MAIN_SCENARIO_NAME, 2d, 0d, 0d),
            List.of("bottle", "s1", 4d, 2d, 2d),
            List.of("bottle", "s2", 1.5d, -2.5d, -0.5d),
            List.of("cookie", MAIN_SCENARIO_NAME, 3d, 0d, 0d),
            List.of("cookie", "s1", 4d, 1d, 1d),
            List.of("cookie", "s2", 2.5d, -1.5d, -0.5d),
            List.of("shirt", MAIN_SCENARIO_NAME, 10d, 0d, 0d),
            List.of("shirt", "s1", 11d, 1d, 1d),
            List.of("shirt", "s2", 9d, -2d, -1d));
  }

  @Test
  void testAbsoluteDifferenceWithPreviousSingleGroupExplicitlyDefined() {
    List<String> elements = List.of("s2", MAIN_SCENARIO_NAME, "s1");
    Measure price = sum("p", this.price);
    ComparisonMeasureReferencePosition priceCompPrev = new ComparisonMeasureReferencePosition(
            "priceCompPrev",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            price,
            Map.of(this.scenario, "s-1"),
            elements);

    ComparisonMeasureReferencePosition priceCompFirst = new ComparisonMeasureReferencePosition(
            "priceCompFirst",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            price,
            Map.of(this.scenario, "first"),
            elements);

    var query = Query
            .from(this.storeName)
            .select(List.of(this.scenario), List.of(price, priceCompPrev, priceCompFirst))
            .orderBy(this.scenario, elements)
            .build();

    Table dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset.headers().stream().map(Header::name)).containsExactly(
            SqlUtils.getFieldFullName(this.storeName, SCENARIO_FIELD_NAME),
            "p",
            "priceCompPrev",
            "priceCompFirst");
    Assertions.assertThat(dataset).containsExactly(
            List.of("s2", 13d, 0d, 0d),
            List.of("base", 15d, 2d, 2d),
            List.of("s1", 19d, 4d, 6d));

    // With a filter
    query = Query
            .from(this.storeName)
            .where(this.scenario, eq("s1"))
            .select(List.of(this.scenario), List.of(price, priceCompPrev, priceCompFirst))
            .orderBy(this.scenario, elements)
            .build();

    dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset.headers().stream().map(Header::name)).containsExactly(
            SqlUtils.getFieldFullName(this.storeName, SCENARIO_FIELD_NAME),
            "p",
            "priceCompPrev",
            "priceCompFirst");
    Assertions.assertThat(dataset).containsExactly(List.of("s1", 19d, 4d, 6d));

    query = Query
            .from(this.storeName)
            .select(List.of(this.scenario, this.ean), List.of(price, priceCompPrev, priceCompFirst))
            .orderBy(this.scenario, elements)
            .build();

    dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset).containsExactly(
            List.of("s2", "bottle", 1.5d, 0d, 0d),
            List.of("s2", "cookie", 2.5d, 0d, 0d),
            List.of("s2", "shirt", 9d, 0d, 0d),
            List.of(MAIN_SCENARIO_NAME, "bottle", 2d, 0.5d, 0.5d),
            List.of(MAIN_SCENARIO_NAME, "cookie", 3d, 0.5d, 0.5d),
            List.of(MAIN_SCENARIO_NAME, "shirt", 10d, 1d, 1d),
            List.of("s1", "bottle", 4d, 2d, 2.5d),
            List.of("s1", "cookie", 4d, 1d, 1.5d),
            List.of("s1", "shirt", 11d, 1d, 2d));

    query = Query
            .from(this.storeName)
            .select(List.of(this.ean, this.scenario), List.of(price, priceCompPrev, priceCompFirst))
            .orderBy(this.scenario, elements)
            .build();

    dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset).containsExactly(
            List.of("bottle", "s2", 1.5d, 0d, 0d),
            List.of("cookie", "s2", 2.5d, 0d, 0d),
            List.of("shirt", "s2", 9d, 0d, 0d),
            List.of("bottle", MAIN_SCENARIO_NAME, 2d, 0.5d, 0.5d),
            List.of("cookie", MAIN_SCENARIO_NAME, 3d, 0.5d, 0.5d),
            List.of("shirt", MAIN_SCENARIO_NAME, 10d, 1d, 1d),
            List.of("bottle", "s1", 4d, 2d, 2.5d),
            List.of("cookie", "s1", 4d, 1d, 1.5d),
            List.of("shirt", "s1", 11d, 1d, 2d));
  }

  /**
   * See comment of {@link ATestPeriodComparison#testCompareYearCurrentWithPreviousWithFilterAndCalculatedMeasure()}
   */
  @Test
  void testCombineGroupComparisonAndPercentOfParent() {
    List<String> elements = List.of(MAIN_SCENARIO_NAME, "s1", "s2");
    Measure price = sum("p", this.price);
    var priceCompPrev = new ComparisonMeasureReferencePosition(
            "priceCompPrev",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            price,
            Map.of(this.scenario, "s-1"),
            elements);

    List<Field> fields = List.of(this.scenario, this.ean);
    var pop = new ComparisonMeasureReferencePosition("parent", DIVIDE, priceCompPrev, fields);

    // use a filter to have three different scopes. One for priceCompPrev, the second for pop and the last one for price
    var query = Query
            .from(this.storeName)
            .where(criterion(this.scenario, Functions.in(MAIN_SCENARIO_NAME, "s1")))
            .select(fields, List.of(price, priceCompPrev, pop))
            .rollup(fields)
            .build();

    Table dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset).containsExactly(
            Arrays.asList(GRAND_TOTAL, GRAND_TOTAL, 34d, null, null),
            Arrays.asList("base", TOTAL, 15d, 0d, null),
            Arrays.asList("base", "bottle", 2d, 0d, Double.NaN),
            Arrays.asList("base", "cookie", 3d, 0d, Double.NaN),
            Arrays.asList("base", "shirt", 10d, 0d, Double.NaN),
            Arrays.asList("s1", TOTAL, 19d, 4d, null),
            Arrays.asList("s1", "bottle", 4d, 2d, 0.5d),
            Arrays.asList("s1", "cookie", 4d, 1d, 0.25d),
            Arrays.asList("s1", "shirt", 11d, 1d, 0.25d));
  }
}
