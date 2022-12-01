package me.paulbares.query;

import me.paulbares.TestClass;
import me.paulbares.query.builder.Query;
import me.paulbares.query.dto.Period;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.*;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static me.paulbares.query.ComparisonMethod.ABSOLUTE_DIFFERENCE;
import static me.paulbares.query.Functions.criterion;
import static me.paulbares.query.Functions.eq;
import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static me.paulbares.transaction.TransactionManager.SCENARIO_FIELD_NAME;

@TestClass(ignore = {"Spark"})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class ATestPeriodComparison extends ATestQuery {

  protected String storeName = "myAwesomeStore";

  @Override
  protected Map<String, List<Field>> getFieldsByStore() {
    Field ean = new Field("ean", String.class);
    Field category = new Field("category", String.class);
    Field sales = new Field("sales", double.class);
    Field qty = new Field("quantity", long.class);
    Field year = new Field("year_sales", long.class); // Use long to make sure we support also long as type
    Field semester = new Field("semester_sales", int.class);
    Field quarter = new Field("quarter_sales", int.class);
    Field month = new Field("month_sales", int.class);
    Field date = new Field("date_sales", LocalDate.class);
    return Map.of(this.storeName, List.of(ean, category, sales, qty, year, semester, quarter, month, date));
  }

  @Override
  protected void loadData() {
    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            // 2022
            new Object[]{"bottle", "drink", 20d, 10, 2022, 1, 1, 1, LocalDate.of(2022, 1, 1)},
            new Object[]{"bottle", "drink", 10d, 5, 2022, 1, 2, 4, LocalDate.of(2022, 4, 1)},
            new Object[]{"bottle", "drink", 20d, 10, 2022, 2, 3, 8, LocalDate.of(2022, 8, 1)},
            new Object[]{"bottle", "drink", 10d, 5, 2022, 2, 4, 12, LocalDate.of(2022, 12, 1)},

            new Object[]{"cookie", "food", 60d, 20, 2022, 1, 1, 2, LocalDate.of(2022, 2, 1)},
            new Object[]{"cookie", "food", 30d, 10, 2022, 1, 2, 5, LocalDate.of(2022, 5, 1)},
            new Object[]{"cookie", "food", 15d, 5, 2022, 2, 3, 9, LocalDate.of(2022, 9, 1)},
            new Object[]{"cookie", "food", 15d, 5, 2022, 2, 4, 11, LocalDate.of(2022, 11, 1)},

            new Object[]{"shirt", "cloth", 20d, 2, 2022, 1, 1, 3, LocalDate.of(2022, 3, 1)},
            new Object[]{"shirt", "cloth", 40d, 4, 2022, 1, 2, 6, LocalDate.of(2022, 6, 1)},
            new Object[]{"shirt", "cloth", 50d, 5, 2022, 2, 3, 7, LocalDate.of(2022, 7, 1)},
            new Object[]{"shirt", "cloth", 10d, 1, 2022, 2, 4, 10, LocalDate.of(2022, 10, 1)},

            // 2023 (same data but 2023)
            new Object[]{"bottle", "drink", 20d, 10, 2023, 1, 1, 1, LocalDate.of(2023, 1, 1)},
            new Object[]{"bottle", "drink", 10d, 5, 2023, 1, 2, 4, LocalDate.of(2023, 4, 1)},
            new Object[]{"bottle", "drink", 20d, 10, 2023, 2, 3, 8, LocalDate.of(2023, 8, 1)},
            new Object[]{"bottle", "drink", 10d, 5, 2023, 2, 4, 12, LocalDate.of(2023, 12, 1)},

            new Object[]{"cookie", "food", 60d, 20, 2023, 1, 1, 2, LocalDate.of(2023, 2, 1)},
            new Object[]{"cookie", "food", 30d, 10, 2023, 1, 2, 5, LocalDate.of(2023, 5, 1)},
            new Object[]{"cookie", "food", 15d, 5, 2023, 2, 3, 9, LocalDate.of(2023, 9, 1)},
            new Object[]{"cookie", "food", 15d, 5, 2023, 2, 4, 11, LocalDate.of(2023, 11, 1)},

            new Object[]{"shirt", "cloth", 20d, 2, 2023, 1, 1, 3, LocalDate.of(2023, 3, 1)},
            new Object[]{"shirt", "cloth", 40d, 4, 2023, 1, 2, 6, LocalDate.of(2023, 6, 1)},
            new Object[]{"shirt", "cloth", 50d, 5, 2023, 2, 3, 7, LocalDate.of(2023, 7, 1)},
            new Object[]{"shirt", "cloth", 10d, 1, 2023, 2, 4, 10, LocalDate.of(2023, 10, 1)}
    ));
  }

  @Test
  @Order(1)
  void testCompareQuarterCurrentWithSamePreviousYear() {
    Period.Quarter period = new Period.Quarter("quarter_sales", "year_sales");
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", "sum");
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of("quarter_sales", "q", "year_sales", "y-1"),
            period);

    var query = Query.from(this.storeName)
            .select(List.of("year_sales", "quarter_sales"), List.of(m, sales))
            .build();

    Table finalTable = this.executor.execute(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2022l, 1, null, 100d),
            Arrays.asList(2022l, 2, null, 80d),
            Arrays.asList(2022l, 3, null, 85d),
            Arrays.asList(2022l, 4, null, 35d),
            Arrays.asList(2023l, 1, 0d, 100d),
            Arrays.asList(2023l, 2, 0d, 80d),
            Arrays.asList(2023l, 3, 0d, 85d),
            Arrays.asList(2023l, 4, 0d, 35d));
    Assertions.assertThat(finalTable.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder(period.year(), period.quarter(), "myMeasure", "sum(sales)");

    // Add a condition and make sure condition is cleared during prefetching.s
    query = Query.from(this.storeName)
            .where("year_sales", eq(2023l))
            .select(List.of("year_sales", "quarter_sales"), List.of(m))
            .build();
    finalTable = this.executor.execute(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2023l, 1, 0d),
            Arrays.asList(2023l, 2, 0d),
            Arrays.asList(2023l, 3, 0d),
            Arrays.asList(2023l, 4, 0d));

    query = Query.from(this.storeName)
            .where("quarter_sales", eq(1))
            .select(List.of("year_sales", "quarter_sales"), List.of(m))
            .build();
    finalTable = this.executor.execute(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2022l, 1, null),
            Arrays.asList(2023l, 1, 0d));
  }

  @Test
  @Order(2)
  void testCompareQuarterCurrentWithPrevious() {
    Period.Quarter period = new Period.Quarter("quarter_sales", "year_sales");
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", "sum");
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of("quarter_sales", "q-1"), // Test here we can omit "year" in the ref. map.
            period);

    var query = Query.from(this.storeName)
            .select(List.of("year_sales", "quarter_sales", SCENARIO_FIELD_NAME), List.of(m, sales))
            .build();

    Table finalTable = this.executor.execute(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2022l, 1, "base", null, 100d),
            Arrays.asList(2022l, 2, "base", -20d, 80d),
            Arrays.asList(2022l, 3, "base", 5d, 85d),
            Arrays.asList(2022l, 4, "base", -50d, 35d),
            Arrays.asList(2023l, 1, "base", 65d, 100d),
            Arrays.asList(2023l, 2, "base", -20d, 80d),
            Arrays.asList(2023l, 3, "base", 5d, 85d),
            Arrays.asList(2023l, 4, "base", -50d, 35d));
    Assertions
            .assertThat(finalTable.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder(TransactionManager.SCENARIO_FIELD_NAME, period.year(), period.quarter(), "myMeasure", "sum(sales)");
  }

  @Test
  @Order(3)
  void testCompareYearCurrentWithPrevious() {
    Period.Year period = new Period.Year("year_sales");
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", "sum");
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of("year_sales", "y-1"),
            period);

    var query = Query.from(this.storeName)
            .select(List.of("year_sales", SCENARIO_FIELD_NAME), List.of(m, sales))
            .build();
    Table finalTable = this.executor.execute(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2022l, "base", null, 300d),
            Arrays.asList(2023l, "base", 0d, 300d));
    Assertions
            .assertThat(finalTable.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder(TransactionManager.SCENARIO_FIELD_NAME, period.year(), "myMeasure", "sum(sales)");
  }

  @Test
  @Order(4)
  void testCompareSemesterCurrentWithPrevious() {
    Period.Semester period = new Period.Semester("semester_sales", "year_sales");
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", "sum");
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of(period.semester(), "s-1", period.year(), "y"),
            period);

    var query = Query.from(this.storeName)
            .select(List.of("year_sales", "semester_sales", SCENARIO_FIELD_NAME), List.of(m, sales))
            .build();

    Table finalTable = this.executor.execute(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2022l, 1, "base", null, 180d),
            Arrays.asList(2022l, 2, "base", -60d, 120d),
            Arrays.asList(2023l, 1, "base", 60d, 180d),
            Arrays.asList(2023l, 2, "base", -60d, 120d));
    Assertions
            .assertThat(finalTable.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder(TransactionManager.SCENARIO_FIELD_NAME, period.year(), period.semester(), "myMeasure", "sum(sales)");
  }

  @Test
  void testCompareMonthCurrentWithPrevious() {
    Period.Month period = new Period.Month("month_sales", "year_sales");
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
            .select(List.of("year_sales", "month_sales", SCENARIO_FIELD_NAME), List.of(m, sales))
            .build();

    Table finalTable = this.executor.execute(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2022l, 1, "base", null, 20d),
            Arrays.asList(2022l, 2, "base", 40d, 60d),
            Arrays.asList(2022l, 12, "base", -5d, 10d),
            Arrays.asList(2023l, 1, "base", 10d, 20d),
            Arrays.asList(2023l, 2, "base", 40d, 60d),
            Arrays.asList(2023l, 12, "base", -5d, 10d));
    Assertions
            .assertThat(finalTable.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder(TransactionManager.SCENARIO_FIELD_NAME, period.year(), period.month(), "myMeasure", "sum(sales)");
  }

  @Test
  void testPeriodIsMissingFromQuery() {
    Period.Year period = new Period.Year("year_sales");
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", "sum");
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of("year_sales", "y-1"),
            period);

    var query = Query.from(this.storeName)
            .select(List.of(SCENARIO_FIELD_NAME), List.of(m))
            .build();
    Assertions.assertThatThrownBy(() -> this.executor.execute(query))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("year_sales is not specified in the query but is used in a comparison measure");
  }
}
