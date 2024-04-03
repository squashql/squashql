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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static io.squashql.query.ComparisonMethod.RELATIVE_DIFFERENCE;
import static io.squashql.query.Functions.eq;
import static io.squashql.query.Functions.sum;
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;
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
    TableTypedField ean = new TableTypedField(this.storeName, "ean", String.class);
    TableTypedField category = new TableTypedField(this.storeName, "category", String.class);
    TableTypedField price = new TableTypedField(this.storeName, "price", double.class);
    TableTypedField qty = new TableTypedField(this.storeName, "quantity", int.class);
    return Map.of(this.storeName, List.of(ean, category, price, qty));
  }

  @Override
  protected void loadData() {
    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"bottle", "drink", 2d, 11},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    this.tm.load("s1", this.storeName, List.of(
            new Object[]{"bottle", "drink", 4d, 9},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    this.tm.load("s2", this.storeName, List.of(
            new Object[]{"bottle", "drink", 1.5d, 12},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testAbsoluteDifferenceWithFirst(boolean fullName) {
    Field scenario = fullName ? new TableField(this.storeName, SCENARIO_FIELD_NAME) : new TableField(SCENARIO_FIELD_NAME);
    GroupColumnSetDto group = new GroupColumnSetDto(groupOfScenario, scenario)
            .withNewGroup("group1", List.of(MAIN_SCENARIO_NAME, "s1"))
            .withNewGroup("group2", List.of(MAIN_SCENARIO_NAME, "s2"))
            .withNewGroup("group3", List.of(MAIN_SCENARIO_NAME, "s1", "s2"));

    AggregatedMeasure price = new AggregatedMeasure("p", "price", "sum");
    ComparisonMeasureReferencePosition priceComp = new ComparisonMeasureReferencePosition(
            "priceDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            price,
            Map.of(
                    scenario, AComparisonExecutor.REF_POS_FIRST,
                    tableField(groupOfScenario), "g"
            ),
            ColumnSetKey.GROUP);
    AggregatedMeasure quantity = new AggregatedMeasure("q", "quantity", "sum");
    ComparisonMeasureReferencePosition quantityComp = new ComparisonMeasureReferencePosition(
            "quantityDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            quantity,
            Map.of(
                    scenario, AComparisonExecutor.REF_POS_FIRST,
                    tableField(groupOfScenario), "g"
            ),
            ColumnSetKey.GROUP);

    var query = Query
            .from(this.storeName)
            .select_(List.of(group), List.of(priceComp, price, quantityComp, quantity))
            .build();

    Table dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset.headers().stream().map(Header::name)).containsExactly(
            groupOfScenario, fullName ? SqlUtils.getFieldFullName(this.storeName, SCENARIO_FIELD_NAME) : SCENARIO_FIELD_NAME,
            "priceDiff", "p",
            "quantityDiff", "q");
    Assertions.assertThat(dataset).containsExactlyInAnyOrder(
            List.of("group1", "base", 0d, 15d, 0l, 34l),
            List.of("group1", "s1", 2d, 17d, -2l, 32l),
            List.of("group2", "base", 0d, 15d, 0l, 34l),
            List.of("group2", "s2", -0.5d, 14.5d, 1l, 35l),
            List.of("group3", "base", 0d, 15d, 0l, 34l),
            List.of("group3", "s1", 2d, 17d, -2l, 32l),
            List.of("group3", "s2", -0.5d, 14.5d, 1l, 35l));

    // Add a condition
    query = Query
            .from(this.storeName)
            .where(scenario, eq("s1"))
            .select_(List.of(group), List.of(priceComp))
            .build();

    dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset).containsExactlyInAnyOrder(
            List.of("group1", "s1", 2d),
            List.of("group3", "s1", 2d));
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
    Assertions.assertThat(dataset).containsExactlyInAnyOrder(
            List.of("group1", MAIN_SCENARIO_NAME, 0d, 15d, 0l, 34l),
            List.of("group1", "s1", 2d, 17d, -2l, 32l),
            List.of("group2", MAIN_SCENARIO_NAME, 0d, 15d, 0l, 34l),
            List.of("group2", "s2", -0.5d, 14.5d, 1l, 35l),
            List.of("group3", MAIN_SCENARIO_NAME, 0d, 15d, 0l, 34l),
            List.of("group3", "s1", 2d, 17d, -2l, 32l),
            List.of("group3", "s2", -2.5, 14.5d, 3l, 35l));
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
    Assertions.assertThat(dataset).containsExactlyInAnyOrder(
            List.of("group1", MAIN_SCENARIO_NAME, 0d, 15d, 0d, 34l),
            List.of("group1", "s1", 0.13333333333333333d, 17d, -0.058823529411764705d, 32l),
            List.of("group2", MAIN_SCENARIO_NAME, 0d, 15d, 0d, 34l),
            List.of("group2", "s2", -0.03333333333333333d, 14.5d, 0.029411764705882353d, 35l),
            List.of("group3", MAIN_SCENARIO_NAME, 0d, 15d, 0d, 34l),
            List.of("group3", "s1", 0.13333333333333333d, 17d, -0.058823529411764705d, 32l),
            List.of("group3", "s2", -0.03333333333333333d, 14.5d, 0.029411764705882353d, 35l));
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
  void testAbsoluteDifferenceWithPreviousSingleGroup() {
    GroupColumnSetDto group = new GroupColumnSetDto(groupOfScenario, this.scenario)
            .withNewGroup("whatever", List.of(MAIN_SCENARIO_NAME, "s1", "s2")); // Name of the group is not important when single group

    Measure price = sum("p", this.price);
    ComparisonMeasureReferencePosition priceComp = new ComparisonMeasureReferencePosition(
            "priceDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            price,
            Map.of(this.scenario, "s-1"),
            ColumnSetKey.GROUP); // FIXME we should be able to pass the list of scenarios to the measure here

    var query = Query
            .from(this.storeName)
            .select_(List.of(group), List.of(priceComp, price))
            .build();

    Table dataset = this.executor.executeQuery(query);
    Assertions.assertThat(dataset.headers().stream().map(Header::name)).containsExactly(
            SqlUtils.getFieldFullName(this.storeName, SCENARIO_FIELD_NAME),
            "priceDiff",
            "p");
    Assertions.assertThat(dataset).containsExactlyInAnyOrder(
            List.of("base", 0d, 15d),
            List.of("s1", 2d, 17d),
            List.of("s2", -2.5d, 14.5d));
  }

  @Test
  void testAbsoluteDifferenceWithPreviousSingleGroupUndefined() {
    Measure price = sum("p", this.price);
    ComparisonMeasureReferencePosition priceCompPrev = new ComparisonMeasureReferencePosition(
            "priceCompPrev",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            price,
            Map.of(this.scenario, "s-1"),
            ColumnSetKey.GROUP);

    ComparisonMeasureReferencePosition priceCompFirst = new ComparisonMeasureReferencePosition(
            "priceCompFirst",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            price,
            Map.of(this.scenario, "first"),
            ColumnSetKey.GROUP);

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
    Assertions.assertThat(dataset).containsExactlyInAnyOrder(
            List.of("base", 15d, 0d, 0d),
            List.of("s1", 17d, 2d, 2d),
            List.of("s2", 14.5d, -2.5d, -.5));
  }
}
