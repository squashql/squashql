package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.PivotTableQueryDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.PivotTable;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import io.squashql.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.squashql.query.Axis.COLUMN;
import static io.squashql.query.Axis.ROW;
import static io.squashql.query.ComparisonMethod.ABSOLUTE_DIFFERENCE;
import static io.squashql.query.ComparisonMethod.DIVIDE;
import static io.squashql.query.Functions.*;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;
import static io.squashql.transaction.DataLoader.MAIN_SCENARIO_NAME;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestPartialHierarchicalMeasureComparison extends ABaseTestQuery {

  private final String storeName = "store" + ATestPartialHierarchicalMeasureComparison.class.getSimpleName().toLowerCase();
  private final TableField city = new TableField(this.storeName, "city");
  private final TableField country = new TableField(this.storeName, "country");
  private final TableField continent = new TableField(this.storeName, "continent");
  private final TableField spendingCategory = new TableField(this.storeName, "spending_category");
  private final TableField amount = new TableField(this.storeName, "amount");

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField city = new TableTypedField(this.storeName, "city", String.class);
    TableTypedField country = new TableTypedField(this.storeName, "country", String.class);
    TableTypedField continent = new TableTypedField(this.storeName, "continent", String.class);
    TableTypedField spendingCategory = new TableTypedField(this.storeName, "spending_category", String.class);
    TableTypedField amount = new TableTypedField(this.storeName, "amount", double.class);
    return Map.of(this.storeName, List.of(city, country, continent, spendingCategory, amount));
  }

  @Override
  protected void loadData() {
    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"paris", "france", "eu", "car", 2d},
            new Object[]{"paris", "france", "eu", "home", 4d},
            new Object[]{"paris", "france", "eu", "hobbies", 1d},
            new Object[]{"lyon", "france", "eu", "car", 1d},
            new Object[]{"lyon", "france", "eu", "home", 2d},
            new Object[]{"lyon", "france", "eu", "hobbies", 1d},
            new Object[]{"london", "uk", "eu", "car", 2d},
            new Object[]{"london", "uk", "eu", "home", 5d},
            new Object[]{"london", "uk", "eu", "hobbies", 3d}
    ));
  }

  @Test
  void testPercentOfParentOfAxis() {
    Measure pop = Functions.sum("amount", this.amount);
    List<Field> fields = List.of(this.continent, this.country, this.city);
    Measure pOp = comparisonMeasureWithParentOfAxis("percentOfParent", DIVIDE, pop, COLUMN);
    QueryDto query = Query
            .from(this.storeName)
            .select(fields, List.of(pop, pOp))
            .rollup(fields)
            .build();

    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList(GRAND_TOTAL, GRAND_TOTAL, GRAND_TOTAL, 21d, 1d),
            Arrays.asList("eu", TOTAL, TOTAL, 21d, 21d / 21d),
            Arrays.asList("eu", "france", TOTAL, 11d, 11d / 21d),
            Arrays.asList("eu", "france", "lyon", 4d, 4d / 11d),
            Arrays.asList("eu", "france", "paris", 7d, 7d / 11d),
            Arrays.asList("eu", "uk", TOTAL, 10d, 10d / 21d),
            Arrays.asList("eu", "uk", "london", 10d, 10d / 10d));
  }

  @Test
  void testPercentOfTotalOfAxis() {
    Measure pop = Functions.sum("amount", this.amount);
    List<Field> fields = List.of(this.continent, this.country, this.city);
    Measure pOp = comparisonMeasureWithTotalOfAxis("percentOfParent", DIVIDE, pop, COLUMN);
    // Binary measure should work. The underlying measure has to be "transformed".
    Measure twice = multiply("twice", comparisonMeasureWithTotalOfAxis("percentOfParent", DIVIDE, pop, COLUMN), integer(2));
    QueryDto query = Query
            .from(this.storeName)
            .select(fields, List.of(pop, pOp, twice))
            .rollup(fields)
            .build();

    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList(GRAND_TOTAL, GRAND_TOTAL, GRAND_TOTAL, 21d, 1d, 2d),
            Arrays.asList("eu", TOTAL, TOTAL, 21d, 21d / 21d, 2d),
            Arrays.asList("eu", "france", TOTAL, 11d, 11d / 21d, 2 * 11d / 21d),
            Arrays.asList("eu", "france", "lyon", 4d, 4d / 21d, 2 * 4d / 21d),
            Arrays.asList("eu", "france", "paris", 7d, 7d / 21d, 2 * 7d / 21d),
            Arrays.asList("eu", "uk", TOTAL, 10d, 10d / 21d, 2 * 10d / 21d),
            Arrays.asList("eu", "uk", "london", 10d, 10d / 21d, 2 * 10d / 21d));
  }

  @Test
  void testCompareWithParentOfColumnPivotTable(TestInfo testInfo) {
    Measure pop = Functions.sum("amount", this.amount);
    List<Field> fields = List.of(this.continent, this.country, this.city);
    Measure pOp = comparisonMeasureWithParentOfAxis("diff", ABSOLUTE_DIFFERENCE, pop, COLUMN);
    QueryDto query = Query
            .from(this.storeName)
            .select(FastList.newList(fields).with(this.spendingCategory), List.of(pop, pOp))
            .build();

    PivotTable result = this.executor.executePivotQuery(new PivotTableQueryDto(query, fields, List.of(this.spendingCategory)));
    verifyResults(testInfo, result);
  }

  @Test
  void testCompareWithTotalOfColumnPivotTable(TestInfo testInfo) {
    Measure pop = Functions.sum("amount", this.amount);
    List<Field> fields = List.of(this.continent, this.country, this.city);
    Measure pOp = comparisonMeasureWithTotalOfAxis("diff", ABSOLUTE_DIFFERENCE, pop, COLUMN);
    QueryDto query = Query
            .from(this.storeName)
            .select(FastList.newList(fields).with(this.spendingCategory), List.of(pop, pOp))
            .build();

    PivotTable result = this.executor.executePivotQuery(new PivotTableQueryDto(query, fields, List.of(this.spendingCategory)));
    verifyResults(testInfo, result);
  }

  @Test
  void testCompareWithTotalOfRowPivotTable(TestInfo testInfo) {
    Measure pop = Functions.sum("amount", this.amount);
    List<Field> fields = List.of(this.continent, this.country, this.city);
    Measure pOp = comparisonMeasureWithTotalOfAxis("diff", ABSOLUTE_DIFFERENCE, pop, ROW);
    QueryDto query = Query
            .from(this.storeName)
            .select(FastList.newList(fields).with(this.spendingCategory), List.of(pop, pOp))
            .build();

    PivotTable result = this.executor.executePivotQuery(new PivotTableQueryDto(query, fields, List.of(this.spendingCategory)));
    verifyResults(testInfo, result);
  }

  @Test
  void testCompareWithParentOfRowPivotTable(TestInfo testInfo) {
    Measure pop = Functions.sum("amount", this.amount);
    List<Field> fields = List.of(this.continent, this.country, this.city);
    Measure pOp = comparisonMeasureWithParentOfAxis("diff", ABSOLUTE_DIFFERENCE, pop, ROW);
    QueryDto query = Query
            .from(this.storeName)
            .select(FastList.newList(fields).with(this.spendingCategory), List.of(pop, pOp))
            .build();

    PivotTable result = this.executor.executePivotQuery(new PivotTableQueryDto(query, fields, List.of(this.spendingCategory)));
    verifyResults(testInfo, result);
  }

  private void verifyResults(TestInfo testInfo, PivotTable pt) {
    TestUtil.verifyResults("partialmeasurecomparison", testInfo, pt);
  }
}
