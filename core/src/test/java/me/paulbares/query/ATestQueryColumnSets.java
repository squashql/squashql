package me.paulbares.query;

import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.BucketColumnSetDto;
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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static me.paulbares.transaction.TransactionManager.SCENARIO_FIELD_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestQueryColumnSets {

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

    this.datastore = createDatastore();
    this.queryEngine = createQueryEngine(this.datastore);
    this.executor = new QueryExecutor(this.queryEngine);
    this.tm = createTransactionManager();

    beforeLoad(List.of(ean, category, sales, qty, year, quarter));

    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            // 2022
            new Object[]{"bottle", "drink", 20d, 10, 2022, 1},
            new Object[]{"bottle", "drink", 10d, 5, 2022, 3},

            new Object[]{"cookie", "food", 60d, 20, 2022, 2},
            new Object[]{"cookie", "food", 30d, 10, 2022, 4},

            // 2023 (same data but 2023)
            new Object[]{"bottle", "drink", 20d, 10, 2023, 1},
            new Object[]{"bottle", "drink", 10d, 5, 2023, 3},

            new Object[]{"cookie", "food", 60d, 20, 2023, 2},
            new Object[]{"cookie", "food", 30d, 10, 2023, 4}
    ));

    this.tm.load("up", this.storeName, List.of(
            // 2022 (+10)
            new Object[]{"bottle", "drink", 30d, 10, 2022, 1},
            new Object[]{"bottle", "drink", 20d, 5, 2022, 3},

            new Object[]{"cookie", "food", 70d, 20, 2022, 2},
            new Object[]{"cookie", "food", 40d, 10, 2022, 4},

            // 2023 (same data but 2023)
            new Object[]{"bottle", "drink", 30d, 10, 2023, 1},
            new Object[]{"bottle", "drink", 20d, 5, 2023, 3},

            new Object[]{"cookie", "food", 70d, 20, 2023, 2},
            new Object[]{"cookie", "food", 40d, 10, 2023, 4}
    ));

    this.tm.load("down", this.storeName, List.of(
            // 2022 (-10)
            new Object[]{"bottle", "drink", 10d, 10, 2022, 1},
            new Object[]{"bottle", "drink", 0d, 5, 2022, 3},

            new Object[]{"cookie", "food", 50d, 20, 2022, 2},
            new Object[]{"cookie", "food", 20d, 10, 2022, 4},

            // 2023 (same data but 2023)
            new Object[]{"bottle", "drink", 10d, 10, 2023, 1},
            new Object[]{"bottle", "drink", 0d, 5, 2023, 3},

            new Object[]{"cookie", "food", 50d, 20, 2023, 2},
            new Object[]{"cookie", "food", 20d, 10, 2023, 4}
    ));
  }

  protected void beforeLoad(List<Field> fields) {
  }

  @Test
  void testBucketColumnSetNoComparison() {
    String groupOfScenario = "Group of scenario";
    BucketColumnSetDto bucketCS = new BucketColumnSetDto(groupOfScenario, SCENARIO_FIELD_NAME)
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "up"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "down"))
            .withNewBucket("group3", List.of(MAIN_SCENARIO_NAME, "down", "up"));
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", AggregationFunction.SUM);

    var query = new QueryDto()
            .table(this.storeName)
            .withColumnSet(ColumnSetKey.BUCKET, bucketCS)
            .withMeasure(sales);

    Table execute = this.executor.execute(query);

    double base = 240d, up = 320d, down = 160d;
    Assertions.assertThat(execute).containsExactlyInAnyOrder(
            List.of("group1", MAIN_SCENARIO_NAME, base),
            List.of("group1", "up", up),
            List.of("group2", MAIN_SCENARIO_NAME, base),
            List.of("group2", "down", down),
            List.of("group3", MAIN_SCENARIO_NAME, base),
            List.of("group3", "up", up),
            List.of("group3", "down", down));

    // Do a CJ with a "regular column"
    query = new QueryDto()
            .table(this.storeName)
            .withColumn("category")
            .withColumnSet(ColumnSetKey.BUCKET, bucketCS)
            .withMeasure(sales);

    execute = this.executor.execute(query);
    Assertions.assertThat(execute).containsExactlyInAnyOrder(
            List.of("group3", MAIN_SCENARIO_NAME, "food", 180d),
            List.of("group3", "up", "food", 220d),
            List.of("group3", "down", "food", 140d),
            List.of("group2", "down", "food", 140d),
            List.of("group2", MAIN_SCENARIO_NAME, "food", 180d),
            List.of("group1", "up", "food", 220d),
            List.of("group1", MAIN_SCENARIO_NAME, "food", 180d),

            List.of("group3", MAIN_SCENARIO_NAME, "drink", 60d),
            List.of("group3", "up", "drink", 100d),
            List.of("group3", "down", "drink", 20d),
            List.of("group2", "down", "drink", 20d),
            List.of("group2", MAIN_SCENARIO_NAME, "drink", 60d),
            List.of("group1", "up", "drink", 100d),
            List.of("group1", MAIN_SCENARIO_NAME, "drink", 60d));
  }

  @Test
  void testBucketAndPeriodColumnSetsNoComparison() {
    Period.Year period = new Period.Year("year_sales");
    PeriodColumnSetDto periodCS = new PeriodColumnSetDto(period);
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", AggregationFunction.SUM);
    String groupOfScenario = "Group of scenario";
    BucketColumnSetDto bucketCS = new BucketColumnSetDto(groupOfScenario, SCENARIO_FIELD_NAME)
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "up"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "down"));

    var query = new QueryDto()
            .table(this.storeName)
            .withColumnSet(ColumnSetKey.BUCKET, bucketCS)
            .withColumnSet(ColumnSetKey.PERIOD, periodCS)
            .withMeasure(sales);

    double base = 120d, up = 160d, down = 80d;
    Table table = this.executor.execute(query);
    Assertions.assertThat(table.count()).isEqualTo(8);
    // we do not assert each row because there are too many. Limit to base scenario.
    Assertions.assertThat(table).contains(
            List.of("group2", MAIN_SCENARIO_NAME, 2022, base),
            List.of("group2", "down", 2022, down),
            List.of("group1", MAIN_SCENARIO_NAME, 2022, base),
            List.of("group1", "up", 2022, up),

            List.of("group2", MAIN_SCENARIO_NAME, 2023, base),
            List.of("group2", "down", 2023, down),
            List.of("group1", MAIN_SCENARIO_NAME, 2023, base),
            List.of("group1", "up", 2023, up));
    Assertions
            .assertThat(table.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder("year_sales", groupOfScenario, SCENARIO_FIELD_NAME, "sum(sales)");
  }

  @Test
  void testBucketAndPeriodColumnSetsComparisonPeriod() {
    Period.Year period = new Period.Year("year_sales");
    PeriodColumnSetDto periodCS = new PeriodColumnSetDto(period);
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", AggregationFunction.SUM);
    String groupOfScenario = "Group of scenario";
    BucketColumnSetDto bucketCS = new BucketColumnSetDto(groupOfScenario, SCENARIO_FIELD_NAME)
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "up"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "down"));

    ComparisonMeasureReferencePosition salesYearComp = new ComparisonMeasureReferencePosition(
            "salesYearComp",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            sales,
            ColumnSetKey.PERIOD,
            Map.of("year_sales", "y-1"));

    var query = new QueryDto()
            .table(this.storeName)
            .withColumnSet(ColumnSetKey.BUCKET, bucketCS)
            .withColumnSet(ColumnSetKey.PERIOD, periodCS)
            .withMeasure(salesYearComp)
            .withMeasure(sales);

    double base = 120d, up = 160d, down = 80d;
    Table table = this.executor.execute(query);
    Assertions.assertThat(table.count()).isEqualTo(8);
    // we do not assert each row because there are too many. Limit to base scenario.
    Assertions.assertThat(table).contains(
            Arrays.asList("group2", MAIN_SCENARIO_NAME, 2022, null, base),
            Arrays.asList("group2", "down", 2022, null, down),
            Arrays.asList("group1", MAIN_SCENARIO_NAME, 2022, null, base),
            Arrays.asList("group1", "up", 2022, null, up),

            List.of("group2", MAIN_SCENARIO_NAME, 2023, 0d, base),
            List.of("group2", "down", 2023, 0d, down),
            List.of("group1", MAIN_SCENARIO_NAME, 2023, 0d, base),
            List.of("group1", "up", 2023, 0d, up));
    Assertions
            .assertThat(table.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder(groupOfScenario, SCENARIO_FIELD_NAME, "year_sales", "salesYearComp", "sum(sales)");
  }

  @Test
  void testBucketAndPeriodColumnSetsComparisonBucket() {
    Period.Year period = new Period.Year("year_sales");
    PeriodColumnSetDto periodCS = new PeriodColumnSetDto(period);
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", AggregationFunction.SUM);
    String groupOfScenario = "Group of scenario";
    BucketColumnSetDto bucketCS = new BucketColumnSetDto(groupOfScenario, SCENARIO_FIELD_NAME)
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "up"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "down"));

    ComparisonMeasureReferencePosition salesYearComp = new ComparisonMeasureReferencePosition(
            "salesYearComp",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            sales,
            ColumnSetKey.BUCKET,
            Map.of(SCENARIO_FIELD_NAME, "s-1", groupOfScenario, "g"));

    var query = new QueryDto()
            .table(this.storeName)
            .withColumnSet(ColumnSetKey.BUCKET, bucketCS)
            .withColumnSet(ColumnSetKey.PERIOD, periodCS)
            .withMeasure(salesYearComp)
            .withMeasure(sales);

    double base = 120d, up = 160d, down = 80d;
    Table table = this.executor.execute(query);
    Assertions.assertThat(table.count()).isEqualTo(8);
    // we do not assert each row because there are too many. Limit to base scenario.
    Assertions.assertThat(table).contains(
            List.of("group2", MAIN_SCENARIO_NAME, 2022, 0d, base),
            List.of("group2", "down", 2022, down - base, down),
            List.of("group1", MAIN_SCENARIO_NAME, 2022, 0d, base),
            List.of("group1", "up", 2022, up - base, up),

            List.of("group2", MAIN_SCENARIO_NAME, 2023, 0d, base),
            List.of("group2", "down", 2023, down - base, down),
            List.of("group1", MAIN_SCENARIO_NAME, 2023, 0d, base),
            List.of("group1", "up", 2023, up - base, up));
    Assertions
            .assertThat(table.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder(groupOfScenario, SCENARIO_FIELD_NAME, "year_sales", "salesYearComp", "sum(sales)");
  }

  @Test
  void testBucketAndPeriodColumnSetsComparisonsPeriodAndBucket() {
    String groupOfScenario = "Group of scenario";
    BucketColumnSetDto bucketCS = new BucketColumnSetDto(groupOfScenario, SCENARIO_FIELD_NAME)
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "up"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "down"));

    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", AggregationFunction.SUM);
    ComparisonMeasureReferencePosition salesGroupComp = new ComparisonMeasureReferencePosition(
            "salesGroupComp",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            sales,
            ColumnSetKey.BUCKET,
            Map.of(SCENARIO_FIELD_NAME, "s-1", groupOfScenario, "g"));

    PeriodColumnSetDto periodCS = new PeriodColumnSetDto(new Period.Year("year_sales"));
    ComparisonMeasureReferencePosition salesYearComp = new ComparisonMeasureReferencePosition(
            "salesYearComp",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            sales,
            ColumnSetKey.PERIOD,
            Map.of("year_sales", "y-1"));

    var query = new QueryDto()
            .table(this.storeName)
            .withColumnSet(ColumnSetKey.BUCKET, bucketCS)
            .withColumnSet(ColumnSetKey.PERIOD, periodCS)
            .withMeasure(salesYearComp)
            .withMeasure(salesGroupComp)
            .withMeasure(sales);

    Table table = this.executor.execute(query);
    double base = 120d, up = 160d, down = 80d;
    Assertions.assertThat(table).contains(
            Arrays.asList("group2", MAIN_SCENARIO_NAME, 2022, null, 0d, base),
            Arrays.asList("group2", "down", 2022, null, down - base, down),
            Arrays.asList("group1", MAIN_SCENARIO_NAME, 2022, null, 0d, base),
            Arrays.asList("group1", "up", 2022, null, up - base, up),

            List.of("group2", MAIN_SCENARIO_NAME, 2023, 0d, 0d, base),
            List.of("group2", "down", 2023, 0d, down - base, down),
            List.of("group1", MAIN_SCENARIO_NAME, 2023, 0d, 0d, base),
            List.of("group1", "up", 2023, 0d, up - base, up));
    Assertions
            .assertThat(table.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder(groupOfScenario, SCENARIO_FIELD_NAME, "year_sales", "salesYearComp", "salesGroupComp", "sum(sales)");
  }
}
