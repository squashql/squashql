package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.Period;
import io.squashql.table.Table;
import io.squashql.transaction.DataLoader;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.squashql.query.ComparisonMethod.ABSOLUTE_DIFFERENCE;
import static io.squashql.query.Functions.criterion;
import static io.squashql.query.Functions.eq;
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;
import static io.squashql.transaction.DataLoader.MAIN_SCENARIO_NAME;
import static io.squashql.transaction.DataLoader.SCENARIO_FIELD_NAME;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class ATestPeriodComparison extends ABaseTestQuery {

  protected String storeName = "store" + getClass().getSimpleName().toLowerCase();

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField ean = new TableTypedField(this.storeName, "ean", String.class);
    TableTypedField category = new TableTypedField(this.storeName, "category", String.class);
    TableTypedField sales = new TableTypedField(this.storeName, "sales", double.class);
    TableTypedField qty = new TableTypedField(this.storeName, "quantity", long.class);
    TableTypedField year = new TableTypedField(this.storeName, "year_sales", long.class); // Use long to make sure we support also long as type
    TableTypedField semester = new TableTypedField(this.storeName, "semester_sales", int.class);
    TableTypedField quarter = new TableTypedField(this.storeName, "quarter_sales", int.class);
    TableTypedField month = new TableTypedField(this.storeName, "month_sales", int.class);
    TableTypedField date = new TableTypedField(this.storeName, "date_sales", LocalDate.class);
    return Map.of(this.storeName, List.of(ean, category, sales, qty, year, semester, quarter, month, date));
  }

  @Override
  protected void loadData() {
    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            // 2022
            new Object[]{"bottle", "drink", 20d, 10l, 2022l, 1, 1, 1, LocalDate.of(2022, 1, 1)},
            new Object[]{"bottle", "drink", 10d, 5l, 2022l, 1, 2, 4, LocalDate.of(2022, 4, 1)},
            new Object[]{"bottle", "drink", 20d, 10l, 2022l, 2, 3, 8, LocalDate.of(2022, 8, 1)},
            new Object[]{"bottle", "drink", 10d, 5l, 2022l, 2, 4, 12, LocalDate.of(2022, 12, 1)},

            new Object[]{"cookie", "food", 60d, 20l, 2022l, 1, 1, 2, LocalDate.of(2022, 2, 1)},
            new Object[]{"cookie", "food", 30d, 10l, 2022l, 1, 2, 5, LocalDate.of(2022, 5, 1)},
            new Object[]{"cookie", "food", 15d, 5l, 2022l, 2, 3, 9, LocalDate.of(2022, 9, 1)},
            new Object[]{"cookie", "food", 15d, 5l, 2022l, 2, 4, 11, LocalDate.of(2022, 11, 1)},

            new Object[]{"shirt", "cloth", 20d, 2l, 2022l, 1, 1, 3, LocalDate.of(2022, 3, 1)},
            new Object[]{"shirt", "cloth", 40d, 4l, 2022l, 1, 2, 6, LocalDate.of(2022, 6, 1)},
            new Object[]{"shirt", "cloth", 50d, 5l, 2022l, 2, 3, 7, LocalDate.of(2022, 7, 1)},
            new Object[]{"shirt", "cloth", 10d, 1l, 2022l, 2, 4, 10, LocalDate.of(2022, 10, 1)},

            // 2023 (same data but 2023)
            new Object[]{"bottle", "drink", 20d, 10l, 2023l, 1, 1, 1, LocalDate.of(2023, 1, 1)},
            new Object[]{"bottle", "drink", 10d, 5l, 2023l, 1, 2, 4, LocalDate.of(2023, 4, 1)},
            new Object[]{"bottle", "drink", 20d, 10l, 2023l, 2, 3, 8, LocalDate.of(2023, 8, 1)},
            new Object[]{"bottle", "drink", 10d, 5l, 2023l, 2, 4, 12, LocalDate.of(2023, 12, 1)},

            new Object[]{"cookie", "food", 60d, 20l, 2023l, 1, 1, 2, LocalDate.of(2023, 2, 1)},
            new Object[]{"cookie", "food", 30d, 10l, 2023l, 1, 2, 5, LocalDate.of(2023, 5, 1)},
            new Object[]{"cookie", "food", 15d, 5l, 2023l, 2, 3, 9, LocalDate.of(2023, 9, 1)},
            new Object[]{"cookie", "food", 15d, 5l, 2023l, 2, 4, 11, LocalDate.of(2023, 11, 1)},

            new Object[]{"shirt", "cloth", 20d, 2l, 2023l, 1, 1, 3, LocalDate.of(2023, 3, 1)},
            new Object[]{"shirt", "cloth", 40d, 4l, 2023l, 1, 2, 6, LocalDate.of(2023, 6, 1)},
            new Object[]{"shirt", "cloth", 50d, 5l, 2023l, 2, 3, 7, LocalDate.of(2023, 7, 1)},
            new Object[]{"shirt", "cloth", 10d, 1l, 2023l, 2, 4, 10, LocalDate.of(2023, 10, 1)}
    ));
  }

  @Test
  void testCompareQuarterCurrentWithSamePreviousYear() {
    Period.Quarter period = new Period.Quarter(tableField("quarter_sales"), tableField("year_sales"));
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", "sum");
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of(period.quarter(), "q", period.year(), "y-1"),
            period);

    var query = Query.from(this.storeName)
            .select(tableFields(List.of("year_sales", "quarter_sales")), List.of(m, sales))
            .build();

    Table finalTable = this.executor.executeQuery(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2022l, translate(1), null, 100d),
            Arrays.asList(2022l, translate(2), null, 80d),
            Arrays.asList(2022l, translate(3), null, 85d),
            Arrays.asList(2022l, translate(4), null, 35d),
            Arrays.asList(2023l, translate(1), 0d, 100d),
            Arrays.asList(2023l, translate(2), 0d, 80d),
            Arrays.asList(2023l, translate(3), 0d, 85d),
            Arrays.asList(2023l, translate(4), 0d, 35d));
    Assertions.assertThat(finalTable.headers().stream().map(Header::name))
            .containsExactlyInAnyOrder(period.year().name(), period.quarter().name(), "myMeasure", "sum(sales)");

    // Add a condition and make sure condition is cleared during prefetching.s
    query = Query.from(this.storeName)
            .where(tableField("year_sales"), eq(2023l))
            .select(tableFields(List.of("year_sales", "quarter_sales")), List.of(m))
            .build();
    finalTable = this.executor.executeQuery(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2023l, translate(1), 0d),
            Arrays.asList(2023l, translate(2), 0d),
            Arrays.asList(2023l, translate(3), 0d),
            Arrays.asList(2023l, translate(4), 0d));

    query = Query.from(this.storeName)
            .where(tableField("quarter_sales"), eq(1))
            .select(tableFields(List.of("year_sales", "quarter_sales")), List.of(m))
            .build();
    finalTable = this.executor.executeQuery(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2022l, translate(1), null),
            Arrays.asList(2023l, translate(1), 0d));
  }

  @Test
  void testCompareQuarterCurrentWithPrevious() {
    Period.Quarter period = new Period.Quarter(tableField("quarter_sales"), tableField("year_sales"));
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", "sum");
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of(period.quarter(), "q-1"), // Test here we can omit "year" in the ref. map.
            period);

    var query = Query.from(this.storeName)
            .select(tableFields(List.of("year_sales", "quarter_sales", SCENARIO_FIELD_NAME)), List.of(m, sales))
            .build();

    Table finalTable = this.executor.executeQuery(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2022l, translate(1), "base", null, 100d),
            Arrays.asList(2022l, translate(2), "base", -20d, 80d),
            Arrays.asList(2022l, translate(3), "base", 5d, 85d),
            Arrays.asList(2022l, translate(4), "base", -50d, 35d),
            Arrays.asList(2023l, translate(1), "base", 65d, 100d),
            Arrays.asList(2023l, translate(2), "base", -20d, 80d),
            Arrays.asList(2023l, translate(3), "base", 5d, 85d),
            Arrays.asList(2023l, translate(4), "base", -50d, 35d));
    Assertions
            .assertThat(finalTable.headers().stream().map(Header::name))
            .containsExactlyInAnyOrder(DataLoader.SCENARIO_FIELD_NAME, period.year().name(), period.quarter().name(), "myMeasure", "sum(sales)");
  }

  @Test
  void testCompareYearCurrentWithPrevious() {
    Period.Year period = new Period.Year(tableField("year_sales"));
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", "sum");
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of(period.year(), "y-1"),
            period);

    var query = Query.from(this.storeName)
            .select(tableFields(List.of("year_sales", SCENARIO_FIELD_NAME)), List.of(m, sales))
            .build();
    Table finalTable = this.executor.executeQuery(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2022l, "base", null, 300d),
            Arrays.asList(2023l, "base", 0d, 300d));
    Assertions
            .assertThat(finalTable.headers().stream().map(Header::name))
            .containsExactlyInAnyOrder(DataLoader.SCENARIO_FIELD_NAME, period.year().name(), "myMeasure", "sum(sales)");

    // Rollup will make Grand Total and Total appear. For this line, we can't make the comparison. Null should be
    // written and the query should not fail.
    query = Query.from(this.storeName)
            .select(tableFields(List.of("year_sales", SCENARIO_FIELD_NAME)), List.of(m, sales))
            .rollup(tableFields(List.of("year_sales", SCENARIO_FIELD_NAME)))
            .build();
    finalTable = this.executor.executeQuery(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(GRAND_TOTAL, GRAND_TOTAL, null, 600d),
            Arrays.asList(2022l, TOTAL, null, 300d),
            Arrays.asList(2022l, "base", null, 300d),
            Arrays.asList(2023l, TOTAL, 0d, 300d),
            Arrays.asList(2023l, "base", 0d, 300d));
  }

  @Test
  void testCompareSemesterCurrentWithPrevious() {
    Period.Semester period = new Period.Semester(tableField("semester_sales"), tableField("year_sales"));
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", "sum");
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of(period.semester(), "s-1", period.year(), "y"),
            period);

    var query = Query.from(this.storeName)
            .select(tableFields(List.of("year_sales", "semester_sales", SCENARIO_FIELD_NAME)), List.of(m, sales))
            .build();

    Table finalTable = this.executor.executeQuery(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2022l, translate(1), "base", null, 180d),
            Arrays.asList(2022l, translate(2), "base", -60d, 120d),
            Arrays.asList(2023l, translate(1), "base", 60d, 180d),
            Arrays.asList(2023l, translate(2), "base", -60d, 120d));
    Assertions
            .assertThat(finalTable.headers().stream().map(Header::name))
            .containsExactlyInAnyOrder(DataLoader.SCENARIO_FIELD_NAME, period.year().name(), period.semester().name(), "myMeasure", "sum(sales)");
  }

  @Test
  void testCompareMonthCurrentWithPrevious() {
    Period.Month period = new Period.Month(tableField("month_sales"), tableField("year_sales"));
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", "sum");
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of(period.month(), "m-1", period.year(), "y"),
            period);
    var query = Query.from(this.storeName)
            // Filter to limit the number of rows
            .where(Functions.all(
                    criterion("year_sales", Functions.in(2022, 2023)),
                    criterion("month_sales", Functions.in(1, 2, 12))
            ))
            .select(tableFields(List.of("year_sales", "month_sales", SCENARIO_FIELD_NAME)), List.of(m, sales))
            .build();

    Table finalTable = this.executor.executeQuery(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2022l, translate(1), "base", null, 20d),
            Arrays.asList(2022l, translate(2), "base", 40d, 60d),
            Arrays.asList(2022l, translate(12), "base", -5d, 10d),
            Arrays.asList(2023l, translate(1), "base", 10d, 20d),
            Arrays.asList(2023l, translate(2), "base", 40d, 60d),
            Arrays.asList(2023l, translate(12), "base", -5d, 10d));
    Assertions
            .assertThat(finalTable.headers().stream().map(Header::name))
            .containsExactlyInAnyOrder(DataLoader.SCENARIO_FIELD_NAME, period.year().name(), period.month().name(), "myMeasure", "sum(sales)");
  }

  @Test
  void testPeriodIsMissingFromQuery() {
    Period.Year period = new Period.Year(tableField("year_sales"));
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", "sum");
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of(period.year(), "y-1"),
            period);

    var query = Query.from(this.storeName)
            .select(tableFields(List.of(SCENARIO_FIELD_NAME)), List.of(m))
            .build();
    Assertions.assertThatThrownBy(() -> this.executor.executeQuery(query))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("TableTypedField[store=null, name=year_sales, type=long, alias=null, cte=false] is not specified in the query but is used in a comparison measure");
  }

  /**
   * FIXME this test does not test what it tested initially due to the fact that multiply measure is not computed in the
   * db directly. We could remove it but I think the query plan execution is not correct after the change PR #193 but I
   * cannot prove it.
   *
   * This test is making sure that the following case is supported:
   * 1) Two execution plans are created. One for the scope of the query with filter (Plan1), another one for the scope of the
   * query without the filter (Plan2).
   * 2) A comparison measure uses a complex measure (evaluated in SquashQL) as underlying.
   * 3) Plan2 is computed THEN Plan1 is computed in that order (dependency of plans) to compute the final values of the
   * comparison measure.
   */
  @Test
  void testCompareYearCurrentWithPreviousWithFilterAndCalculatedMeasure() {
    Period.Year period = new Period.Year(tableField("year_sales"));
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", "sum");
    Measure multiply = Functions.multiply("sales times 2", sales, Functions.integer(2));
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            multiply,
            Map.of(period.year(), "y-1"),
            period);

    var query = Query.from(this.storeName)
            .where(criterion("year_sales", eq(2023l)))
            .select(tableFields(List.of("year_sales", SCENARIO_FIELD_NAME)), List.of(m, multiply))
            .build();
    Table finalTable = this.executor.executeQuery(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2023l, "base", 0d, 600d));
  }

  @Test
  void testCompareWithLimit() {
    Period.Quarter period = new Period.Quarter(tableField("quarter_sales"), tableField("year_sales"));
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", "sum");
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of(period.quarter(), "q", period.year(), "y-1"),
            period);

    var query = Query.from(this.storeName)
            .select(tableFields(List.of("year_sales", "quarter_sales")), List.of(m, sales))
            .limit(2)
            .build();

    Assertions.assertThatThrownBy(() -> this.executor.executeQuery(query)).hasMessageContaining("Too many rows");
  }

  @Test
  void testYearFunctionAndHaving() {
    // BigQuery. See https://github.com/squashql/squashql/issues/183
    Assumptions.assumeFalse(this.queryEngine.getClass().getSimpleName().contains(TestClass.Type.BIGQUERY.className));

    String dateSales = "date_sales";
    var query = Query.from(this.storeName)
            .select(List.of(Functions.year(dateSales)), List.of(CountMeasure.INSTANCE))
            .having(criterion(Functions.year(dateSales), eq(2022)))
            .build();
    final Table finalTable = this.executor.executeQuery(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            List.of(yearType(2022), 12L));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testYearFunctionAndWhere(boolean fullName) {
    String dateSales = fullName ? SqlUtils.getFieldFullName(this.storeName, "date_sales") : "date_sales";
    var query = Query.from(this.storeName)
            .where(criterion(Functions.year(dateSales), eq(2022)))
            .select(List.of(Functions.year(dateSales)), List.of(CountMeasure.INSTANCE))
            .build();
    final Table finalTable = this.executor.executeQuery(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            List.of(yearType(2022), 12L));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testQuarterFunctionAndWhere(boolean fullName) {
    String dateSales = fullName ? SqlUtils.getFieldFullName(this.storeName, "date_sales") : "date_sales";
    var query = Query.from(this.storeName)
            .where(criterion(Functions.year(dateSales), eq(2022)))
            .select(List.of(Functions.year(dateSales), Functions.quarter(dateSales)), List.of(CountMeasure.INSTANCE))
            .build();
    final Table finalTable = this.executor.executeQuery(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            List.of(yearType(2022), quarterType(1), 3L),
            List.of(yearType(2022), quarterType(2), 3L),
            List.of(yearType(2022), quarterType(3), 3L),
            List.of(yearType(2022), quarterType(4), 3L));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testMonthFunctionAndWhere(boolean fullName) {
    String dateSales = fullName ? SqlUtils.getFieldFullName(this.storeName, "date_sales") : "date_sales";
    var query = Query.from(this.storeName)
            .where(criterion(Functions.year(dateSales), eq(2022)))
            .select(List.of(Functions.year(dateSales), Functions.month(dateSales)), List.of(CountMeasure.INSTANCE))
            .build();
    final Table finalTable = this.executor.executeQuery(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            List.of(yearType(2022), monthType(1), 1L),
            List.of(yearType(2022), monthType(2), 1L),
            List.of(yearType(2022), monthType(3), 1L),
            List.of(yearType(2022), monthType(4), 1L),
            List.of(yearType(2022), monthType(5), 1L),
            List.of(yearType(2022), monthType(6), 1L),
            List.of(yearType(2022), monthType(7), 1L),
            List.of(yearType(2022), monthType(8), 1L),
            List.of(yearType(2022), monthType(9), 1L),
            List.of(yearType(2022), monthType(10), 1L),
            List.of(yearType(2022), monthType(11), 1L),
            List.of(yearType(2022), monthType(12), 1L));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDateFunctionWithRollup(boolean fullName) {
    String dateSales = fullName ? SqlUtils.getFieldFullName(this.storeName, "date_sales") : "date_sales";
    var query = Query.from(this.storeName)
            .select(List.of(Functions.year(dateSales)), List.of(CountMeasure.INSTANCE))
            .rollup(Functions.year(dateSales))
            .build();
    final Table finalTable = this.executor.executeQuery(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            List.of(yearType(2022), 12L),
            List.of(yearType(2023), 12L),
            List.of("Grand Total", 24L));
  }

  protected Object yearType(int i) {
    String qesn = this.queryEngine.getClass().getSimpleName();
    if (qesn.contains(TestClass.Type.CLICKHOUSE.className) || qesn.contains(TestClass.Type.SPARK.className)) {
      return i;
    } else {
      return (long) i;
    }
  }

  protected Object quarterType(int i) {
    String qesn = this.queryEngine.getClass().getSimpleName();
    if (qesn.contains(TestClass.Type.CLICKHOUSE.className) || qesn.contains(TestClass.Type.SPARK.className)) {
      return i;
    } else {
      return (long) i;
    }
  }

  protected Object monthType(int i) {
    String qesn = this.queryEngine.getClass().getSimpleName();
    if (qesn.contains(TestClass.Type.CLICKHOUSE.className) || qesn.contains(TestClass.Type.SPARK.className)) {
      return i;
    } else {
      return (long) i;
    }
  }
}
