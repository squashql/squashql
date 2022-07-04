package me.paulbares.query;

import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.Period;
import me.paulbares.query.dto.PeriodColumnSetDto;
import me.paulbares.query.dto.QueryDto;
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

import static me.paulbares.query.ComparisonMethod.ABSOLUTE_DIFFERENCE;
import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static me.paulbares.transaction.TransactionManager.SCENARIO_FIELD_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestPeriodComparison {

  protected Datastore datastore;

  protected QueryEngine queryEngine;

  protected QueryExecutor executor;

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
    Field quarter = new Field("quarter_sales", int.class);
    Field month = new Field("month_sales", int.class);
    Field date = new Field("date_sales", LocalDate.class);

    this.datastore = createDatastore();
    this.queryEngine = createQueryEngine(this.datastore);
    this.executor = new QueryExecutor(this.queryEngine);
    this.tm = createTransactionManager();

    beforeLoading(List.of(ean, category, sales, qty, year, quarter, month, date));

    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            // 2022
            new Object[]{"bottle", "drink", 20d, 10, 2022, 1, 1, LocalDate.of(2022, 1, 1)},
            new Object[]{"bottle", "drink", 10d, 5, 2022, 2, 4, LocalDate.of(2022, 4, 1)},
            new Object[]{"bottle", "drink", 20d, 10, 2022, 3, 8, LocalDate.of(2022, 8, 1)},
            new Object[]{"bottle", "drink", 10d, 5, 2022, 4, 12, LocalDate.of(2022, 12, 1)},

            new Object[]{"cookie", "food", 60d, 20, 2022, 1, 2, LocalDate.of(2022, 2, 1)},
            new Object[]{"cookie", "food", 30d, 10, 2022, 2, 5, LocalDate.of(2022, 5, 1)},
            new Object[]{"cookie", "food", 15d, 5, 2022, 3, 9, LocalDate.of(2022, 9, 1)},
            new Object[]{"cookie", "food", 15d, 5, 2022, 4, 11, LocalDate.of(2022, 11, 1)},

            new Object[]{"shirt", "cloth", 20d, 2, 2022, 1, 3, LocalDate.of(2022, 3, 1)},
            new Object[]{"shirt", "cloth", 40d, 4, 2022, 2, 6, LocalDate.of(2022, 6, 1)},
            new Object[]{"shirt", "cloth", 50d, 5, 2022, 3, 7, LocalDate.of(2022, 7, 1)},
            new Object[]{"shirt", "cloth", 10d, 1, 2022, 4, 10, LocalDate.of(2022, 10, 1)},

            // 2023 (same data but 2023)
            new Object[]{"bottle", "drink", 20d, 10, 2023, 1, 1, LocalDate.of(2023, 1, 1)},
            new Object[]{"bottle", "drink", 10d, 5, 2023, 2, 4, LocalDate.of(2023, 4, 1)},
            new Object[]{"bottle", "drink", 20d, 10, 2023, 3, 8, LocalDate.of(2023, 8, 1)},
            new Object[]{"bottle", "drink", 10d, 5, 2023, 4, 12, LocalDate.of(2023, 12, 1)},

            new Object[]{"cookie", "food", 60d, 20, 2023, 1, 2, LocalDate.of(2023, 2, 1)},
            new Object[]{"cookie", "food", 30d, 10, 2023, 2, 5, LocalDate.of(2023, 5, 1)},
            new Object[]{"cookie", "food", 15d, 5, 2023, 3, 9, LocalDate.of(2023, 9, 1)},
            new Object[]{"cookie", "food", 15d, 5, 2023, 4, 11, LocalDate.of(2023, 11, 1)},

            new Object[]{"shirt", "cloth", 20d, 2, 2023, 1, 3, LocalDate.of(2023, 3, 1)},
            new Object[]{"shirt", "cloth", 40d, 4, 2023, 2, 6, LocalDate.of(2023, 6, 1)},
            new Object[]{"shirt", "cloth", 50d, 5, 2023, 3, 7, LocalDate.of(2023, 7, 1)},
            new Object[]{"shirt", "cloth", 10d, 1, 2023, 4, 10, LocalDate.of(2023, 10, 1)}
    ));
  }

  protected void beforeLoading(List<Field> fields) {
  }

  @Test
  void testCompareQuarterCurrentWithSamePreviousYear() {
    Period.Quarter period = new Period.Quarter("quarter_sales", "year_sales");
    AggregatedMeasure sales = new AggregatedMeasure("sales", "sum");
    ComparisonMeasure m = QueryBuilder.periodComparison(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of(
                    "quarter_sales", "q",
                    "year_sales", "y-1"
            ));
    PeriodColumnSetDto periodCS = new PeriodColumnSetDto(period);

    var query = new QueryDto()
            .table(this.storeName)
            .withColumnSet(QueryDto.PERIOD, periodCS)
            .withMeasure(m)
            .withMeasure(sales);

    Table finalTable = this.executor.execute(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2022, 1, null, 100d),
            Arrays.asList(2022, 2, null, 80d),
            Arrays.asList(2022, 3, null, 85d),
            Arrays.asList(2022, 4, null, 35d),
            Arrays.asList(2023, 1, 0d, 100d),
            Arrays.asList(2023, 2, 0d, 80d),
            Arrays.asList(2023, 3, 0d, 85d),
            Arrays.asList(2023, 4, 0d, 35d));
    Assertions
            .assertThat(finalTable.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder(period.year(), period.quarter(), "myMeasure", "sum(sales)");
  }

  @Test
  void testCompareQuarterCurrentWithPrevious() {
    Period.Quarter period = new Period.Quarter("quarter_sales", "year_sales");
    AggregatedMeasure sales = new AggregatedMeasure("sales", "sum");
    ComparisonMeasure m = QueryBuilder.periodComparison(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of(
                    "quarter_sales", "q-1",
                    "year_sales", "y"
            ));
    PeriodColumnSetDto periodCS = new PeriodColumnSetDto(period);

    var query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .withColumnSet(QueryDto.PERIOD, periodCS)
            .withMeasure(m)
            .withMeasure(sales);

    Table finalTable = this.executor.execute(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2022, 1, "base", null, 100d),
            Arrays.asList(2022, 2, "base", -20d, 80d),
            Arrays.asList(2022, 3, "base", 5d, 85d),
            Arrays.asList(2022, 4, "base", -50d, 35d),
            Arrays.asList(2023, 1, "base", 65d, 100d),
            Arrays.asList(2023, 2, "base", -20d, 80d),
            Arrays.asList(2023, 3, "base", 5d, 85d),
            Arrays.asList(2023, 4, "base", -50d, 35d));
    Assertions
            .assertThat(finalTable.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder(TransactionManager.SCENARIO_FIELD_NAME, period.year(), period.quarter(), "myMeasure", "sum(sales)");
  }

  @Test
  void testCompareYearCurrentWithPrevious() {
    Period.Year period = new Period.Year("year_sales");
    AggregatedMeasure sales = new AggregatedMeasure("sales", "sum");
    ComparisonMeasure m = QueryBuilder.periodComparison(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of("year_sales", "y-1"));
    PeriodColumnSetDto periodCS = new PeriodColumnSetDto(period);

    var query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .withColumnSet(QueryDto.PERIOD, periodCS)
            .withMeasure(m)
            .withMeasure(sales);

    Table finalTable = this.executor.execute(query);
    Assertions.assertThat(finalTable).containsExactlyInAnyOrder(
            Arrays.asList(2022, "base", null, 300d),
            Arrays.asList(2023, "base", 0d, 300d));
    Assertions
            .assertThat(finalTable.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder(TransactionManager.SCENARIO_FIELD_NAME, period.year(), "myMeasure", "sum(sales)");
  }
}
