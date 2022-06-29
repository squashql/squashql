package me.paulbares.query;

import me.paulbares.NewQueryExecutor;
import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.comp.BinaryOperations;
import me.paulbares.query.dto.*;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;
import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestNewQueryExecutor {

  protected Datastore datastore;

  protected QueryEngine queryEngine;

  protected NewQueryExecutor executor;

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
    this.executor = new NewQueryExecutor(this.queryEngine);
    this.tm = createTransactionManager();

    beforeLoading(List.of(ean, category, sales, qty, year, quarter));

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

  protected void beforeLoading(List<Field> fields) {
  }

  // TODO test in separated class with comparison api like scenariogrouping executor
  @Test
  void testBucketColumnSet() {
    String groupOfScenario = "Group of scenario";
    BucketColumnSetDto bucketCS = new BucketColumnSetDto(groupOfScenario, SCENARIO_FIELD_NAME)
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "up"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "down"))
            .withNewBucket("group3", List.of(MAIN_SCENARIO_NAME, "down", "up"));
    AggregatedMeasure sales = new AggregatedMeasure("sales", AggregationFunction.SUM);

    var query = new NewQueryDto()
            .table(this.storeName)
            .withColumnSet(NewQueryDto.BUCKET, bucketCS)
            .withMetric(sales);

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
    query = new NewQueryDto()
            .table(this.storeName)
            .withColumn("category")
            .withColumnSet(NewQueryDto.BUCKET, bucketCS)
            .withMetric(sales);

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
  void testBucketAndPeriodColumnSets() {
    Period.Year period = new Period.Year("year_sales");
    PeriodColumnSetDto periodCS = new PeriodColumnSetDto(period);
    AggregatedMeasure sales = new AggregatedMeasure("sales", AggregationFunction.SUM);
    String groupOfScenario = "Group of scenario";
    BucketColumnSetDto bucketCS = new BucketColumnSetDto(groupOfScenario, SCENARIO_FIELD_NAME)
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "up"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "down"));

    var query = new NewQueryDto()
            .table(this.storeName)
            .withColumnSet(NewQueryDto.BUCKET, bucketCS)
            .withColumnSet(NewQueryDto.PERIOD, periodCS)
            .withMetric(sales);

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
  void test() {
    String groupOfScenario = "Group of scenario";
    BucketColumnSetDto bucketCS = new BucketColumnSetDto(groupOfScenario, SCENARIO_FIELD_NAME)
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "up"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "down"))
            .withNewBucket("group3", List.of(MAIN_SCENARIO_NAME, "down", "up"));
    PeriodColumnSetDto periodCS = new PeriodColumnSetDto(new Period.Quarter("quarter_sales", "year_sales"));

    AggregatedMeasure sales = new AggregatedMeasure("sales", AggregationFunction.SUM);
    BinaryOperationMeasure salesGroupComp = new BinaryOperationMeasure(
            "salesGroupComp",
            BinaryOperations.ABS_DIFF,
            sales,
            Map.of(
                    SCENARIO_FIELD_NAME, "s-1",
                    groupOfScenario, "g"
            ));

    BinaryOperationMeasure salesYearComp = new BinaryOperationMeasure(
            "salesYearComp",
            BinaryOperations.DIVIDE,
            sales,
            Map.of(
                    BinaryOperationMeasure.PeriodUnit.QUARTER.name(), "q",
                    BinaryOperationMeasure.PeriodUnit.YEAR.name(), "y-1"
            ));

    BinaryOperationMeasure myMeasureGroupComp = new BinaryOperationMeasure(
            "myMeasureGroupComp",
            BinaryOperations.ABS_DIFF,
            salesYearComp,
            Map.of(
                    SCENARIO_FIELD_NAME, "s-1",
                    groupOfScenario, "g"
            ));

    var query = new NewQueryDto()
            .table(this.storeName)
            .withColumnSet(NewQueryDto.BUCKET, bucketCS)
            .withColumnSet(NewQueryDto.PERIOD, periodCS)
//            .withMetric(myMeasureGroupComp)
//            .withMetric(salesGroupComp)
            .withMetric(sales);

    Table execute = this.executor.execute(query);
    System.out.println(execute);
  }
}
