package me.paulbares.query;

import me.paulbares.query.comp.Comparisons;
import me.paulbares.query.dto.Period;
import me.paulbares.query.dto.PeriodBucketingQueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestPeriodBucketing {

  protected Datastore datastore;

  protected QueryEngine queryEngine;

  protected PeriodBucketingExecutor executor;

  protected TransactionManager tm;

  protected String storeName = "myAwesomeStore";

  protected abstract QueryEngine createQueryEngine(Datastore datastore);

  protected abstract Datastore createDatastore();

  protected abstract TransactionManager createTransactionManager();

  @BeforeAll
  void setup() {
    Field ean = new Field("ean", String.class);
    Field category = new Field("category", String.class);
    Field sales = new Field("sales", double.class);
    Field qty = new Field("quantity", long.class);
    Field year = new Field("year_sales", int.class);
    Field month = new Field("month_sales", int.class);
    Field date = new Field("date_sales", LocalDate.class);

    this.datastore = createDatastore();
    this.queryEngine = createQueryEngine(this.datastore);
    this.executor = new PeriodBucketingExecutor(this.queryEngine);
    this.tm = createTransactionManager();

    beforeLoading(List.of(ean, category, sales, qty, year, month, date));

    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            // 2022
            new Object[]{"bottle", "drink", 20d, 10, 2022, 1, LocalDate.of(2022, 1, 1)},
            new Object[]{"bottle", "drink", 10d, 5, 2022, 4, LocalDate.of(2022, 4, 1)},
            new Object[]{"bottle", "drink", 20d, 10, 2022, 8, LocalDate.of(2022, 8, 1)},
            new Object[]{"bottle", "drink", 10d, 5, 2022, 12, LocalDate.of(2022, 12, 1)},

            new Object[]{"cookie", "food", 60d, 20, 2022, 2, LocalDate.of(2022, 2, 1)},
            new Object[]{"cookie", "food", 30d, 10, 2022, 5, LocalDate.of(2022, 5, 1)},
            new Object[]{"cookie", "food", 15d, 5, 2022, 9, LocalDate.of(2022, 9, 1)},
            new Object[]{"cookie", "food", 15d, 5, 2022, 11, LocalDate.of(2022, 11, 1)},

            new Object[]{"shirt", "cloth", 20d, 2, 2022, 3, LocalDate.of(2022, 3, 1)},
            new Object[]{"shirt", "cloth", 40d, 4, 2022, 6, LocalDate.of(2022, 6, 1)},
            new Object[]{"shirt", "cloth", 50d, 5, 2022, 7, LocalDate.of(2022, 7, 1)},
            new Object[]{"shirt", "cloth", 10d, 1, 2022, 10, LocalDate.of(2022, 10, 1)},

            // 2023 (same data but 2023)
            new Object[]{"bottle", "drink", 20d, 10, 2023, 1, LocalDate.of(2023, 1, 1)},
            new Object[]{"bottle", "drink", 10d, 5, 2023, 4, LocalDate.of(2023, 4, 1)},
            new Object[]{"bottle", "drink", 20d, 10, 2023, 8, LocalDate.of(2023, 8, 1)},
            new Object[]{"bottle", "drink", 10d, 5, 2023, 12, LocalDate.of(2023, 12, 1)},

            new Object[]{"cookie", "food", 60d, 20, 2023, 2, LocalDate.of(2023, 2, 1)},
            new Object[]{"cookie", "food", 30d, 10, 2023, 5, LocalDate.of(2023, 5, 1)},
            new Object[]{"cookie", "food", 15d, 5, 2023, 9, LocalDate.of(2023, 9, 1)},
            new Object[]{"cookie", "food", 15d, 5, 2023, 11, LocalDate.of(2023, 11, 1)},

            new Object[]{"shirt", "cloth", 20d, 2, 2023, 3, LocalDate.of(2023, 3, 1)},
            new Object[]{"shirt", "cloth", 40d, 4, 2023, 6, LocalDate.of(2023, 6, 1)},
            new Object[]{"shirt", "cloth", 50d, 5, 2023, 7, LocalDate.of(2023, 7, 1)},
            new Object[]{"shirt", "cloth", 10d, 1, 2023, 10, LocalDate.of(2023, 10, 1)}
    ));
  }

  protected void beforeLoading(List<Field> fields) {
  }

  @Test
  void testBucketingQuarterFromMonthYear() {
    testBucketingQuarter(new Period.QuarterFromMonthYear("month_sales", "year_sales"), "year_sales", "quarter");
  }

  @Test
  void testBucketingQuarterFromDate() {
    testBucketingQuarter(new Period.QuarterFromDate("date_sales"), "year", "quarter");
  }

  void testBucketingQuarter(Period period, String expectedHeaderYear, String expectedHeadQuarter) {
    AggregatedMeasure sales = new AggregatedMeasure("sales", "sum");
    ComparisonMeasure m = new ComparisonMeasure(
            "myMeasure",
            Comparisons.COMPARISON_METHOD_ABS_DIFF,
            sales,
            Map.of(
                    ComparisonMeasure.PeriodUnit.QUARTER, "q",
                    ComparisonMeasure.PeriodUnit.YEAR, "y-1"
            ));
    var query = new PeriodBucketingQueryDto()
            .table(this.storeName)
            .wildcardCoordinate(Datastore.SCENARIO_FIELD_NAME)
            .period(period)
            .withMeasure(m)
            .withMeasure(sales);

    PeriodBucketingExecutor.Holder result = this.executor.executeBucketing(query);
    Assertions.assertThat(result.table()).containsExactlyInAnyOrder(
            List.of("base", 2022, 1, 100d),
            List.of("base", 2022, 2, 80d),
            List.of("base", 2022, 3, 85d),
            List.of("base", 2022, 4, 35d),
            List.of("base", 2023, 1, 100d),
            List.of("base", 2023, 2, 80d),
            List.of("base", 2023, 3, 85d),
            List.of("base", 2023, 4, 35d));
    Assertions
            .assertThat(result.table().headers().stream().map(Field::name))
            .containsExactlyInAnyOrder(Datastore.SCENARIO_FIELD_NAME, expectedHeaderYear, expectedHeadQuarter, "sum(sales)");
  }

  @Test
  void testCompareCurrentQuarterWithCurrentQuarterPreviousYearWithQuarterFromMonthYear() {
    testCompareCurrentQuarterWithCurrentQuarterPreviousYear(
            new Period.QuarterFromMonthYear("month_sales", "year_sales"),
            "year_sales",
            "quarter");
  }

  @Test
  void testCompareCurrentQuarterWithCurrentQuarterPreviousYearWithQuarterFromDate() {
    testCompareCurrentQuarterWithCurrentQuarterPreviousYear(
            new Period.QuarterFromDate("date_sales"),
            "year",
            "quarter");
  }

  void testCompareCurrentQuarterWithCurrentQuarterPreviousYear(Period period, String expectedHeaderYear, String expectedHeadQuarter) {
    AggregatedMeasure sales = new AggregatedMeasure("sales", "sum");
    ComparisonMeasure m = new ComparisonMeasure(
            "myMeasure",
            Comparisons.COMPARISON_METHOD_ABS_DIFF,
            sales,
            Map.of(
                    ComparisonMeasure.PeriodUnit.QUARTER, "q",
                    ComparisonMeasure.PeriodUnit.YEAR, "y-1"
            ));

    var query = new PeriodBucketingQueryDto()
            .table(this.storeName)
            .wildcardCoordinate(Datastore.SCENARIO_FIELD_NAME)
            .period(period)
            .withMeasure(m)
            .withMeasure(sales);

    Table finalTable = this.executor.execute(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList("base", 2022, 1, null, 100d),
            Arrays.asList("base", 2022, 2, null, 80d),
            Arrays.asList("base", 2022, 3, null, 85d),
            Arrays.asList("base", 2022, 4, null, 35d),
            Arrays.asList("base", 2023, 1, 0d, 100d),
            Arrays.asList("base", 2023, 2, 0d, 80d),
            Arrays.asList("base", 2023, 3, 0d, 85d),
            Arrays.asList("base", 2023, 4, 0d, 35d));
    Assertions
            .assertThat(finalTable.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder(Datastore.SCENARIO_FIELD_NAME, expectedHeaderYear, expectedHeadQuarter, "myMeasure", "sum(sales)");
  }

  @Test
  void testCompareCurrentQuarterWithPreviousQuarterWithQuarterFromMonthYear() {
    testCompareCurrentQuarterWithPreviousQuarter(
            new Period.QuarterFromMonthYear("month_sales", "year_sales"),
            "year_sales",
            "quarter");
  }

  @Test
  void testCompareCurrentQuarterWithPreviousQuarterWithQuarterFromMonthDate() {
    testCompareCurrentQuarterWithPreviousQuarter(
            new Period.QuarterFromDate("date_sales"),
            "year",
            "quarter");
  }

  void testCompareCurrentQuarterWithPreviousQuarter(Period period, String expectedHeaderYear, String expectedHeadQuarter) {
    AggregatedMeasure sales = new AggregatedMeasure("sales", "sum");
    ComparisonMeasure m = new ComparisonMeasure(
            "myMeasure",
            Comparisons.COMPARISON_METHOD_ABS_DIFF,
            sales,
            Map.of(
                    ComparisonMeasure.PeriodUnit.QUARTER, "q-1",
                    ComparisonMeasure.PeriodUnit.YEAR, "y"
            ));

    var query = new PeriodBucketingQueryDto()
            .table(this.storeName)
            .wildcardCoordinate(Datastore.SCENARIO_FIELD_NAME)
            .period(period)
            .withMeasure(m)
            .withMeasure(sales);

    Table finalTable = this.executor.execute(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList("base", 2022, 1, null, 100d),
            Arrays.asList("base", 2022, 2, -20d, 80d),
            Arrays.asList("base", 2022, 3, 5d, 85d),
            Arrays.asList("base", 2022, 4, -50d, 35d),
            Arrays.asList("base", 2023, 1, 65d, 100d),
            Arrays.asList("base", 2023, 2, -20d, 80d),
            Arrays.asList("base", 2023, 3, 5d, 85d),
            Arrays.asList("base", 2023, 4, -50d, 35d));
    Assertions
            .assertThat(finalTable.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder(Datastore.SCENARIO_FIELD_NAME, expectedHeaderYear, expectedHeadQuarter, "myMeasure", "sum(sales)");
  }

  @Test
  void testCompareCurrentYearWithPreviousYearWithYearFromDate() {
    testCompareCurrentYearWithPreviousYearWithYearFromDate(new Period.YearFromDate("date_sales"), "year");
  }

  @Test
  void testCompareCurrentYearWithPreviousYearWithYear() {
    testCompareCurrentYearWithPreviousYearWithYearFromDate(new Period.Year("year_sales"), "year_sales");
  }

  void testCompareCurrentYearWithPreviousYearWithYearFromDate(Period period, String expectedHeaderYear) {
    AggregatedMeasure sales = new AggregatedMeasure("sales", "sum");
    ComparisonMeasure m = new ComparisonMeasure(
            "myMeasure",
            Comparisons.COMPARISON_METHOD_ABS_DIFF,
            sales,
            Map.of(ComparisonMeasure.PeriodUnit.YEAR, "y-1"));

    var query = new PeriodBucketingQueryDto()
            .table(this.storeName)
            .wildcardCoordinate(Datastore.SCENARIO_FIELD_NAME)
            .period(period)
            .withMeasure(m)
            .withMeasure(sales);

    Table finalTable = this.executor.execute(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList("base", 2022, null, 300d),
            Arrays.asList("base", 2023, 0d, 300d));
    Assertions
            .assertThat(finalTable.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder(Datastore.SCENARIO_FIELD_NAME, expectedHeaderYear, "myMeasure", "sum(sales)");
  }
}
