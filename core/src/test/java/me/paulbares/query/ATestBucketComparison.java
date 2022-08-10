package me.paulbares.query;

import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.BucketColumnSetDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static me.paulbares.query.ComparisonMethod.RELATIVE_DIFFERENCE;
import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static me.paulbares.transaction.TransactionManager.SCENARIO_FIELD_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestBucketComparison {

  protected QueryExecutor executor;
  protected Datastore datastore;
  protected TransactionManager tm;

  protected String storeName = "storeName";

  protected abstract QueryEngine createQueryEngine(Datastore datastore);

  protected abstract Datastore createDatastore();

  protected abstract TransactionManager createTransactionManager();

  protected Map<String, List<String>> groups = new LinkedHashMap<>();

  {
    this.groups.put("group1", List.of("base", "s1"));
    this.groups.put("group2", List.of("base", "s2"));
    this.groups.put("group3", List.of("base", "s1", "s2"));
  }

  protected String groupOfScenario = "Group of scenario";
  protected BucketColumnSetDto bucketCS = new BucketColumnSetDto(this.groupOfScenario, SCENARIO_FIELD_NAME)
          .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "s1"))
          .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "s2"))
          .withNewBucket("group3", List.of(MAIN_SCENARIO_NAME, "s1", "s2"));

  @BeforeAll
  void setup() {
    Field ean = new Field("ean", String.class);
    Field category = new Field("category", String.class);
    Field price = new Field("price", double.class);
    Field qty = new Field("quantity", int.class);

    List<Field> fields = List.of(ean, category, price, qty);
    this.datastore = createDatastore();
    QueryEngine queryEngine = createQueryEngine(this.datastore);
    this.executor = new QueryExecutor(queryEngine);
    this.tm = createTransactionManager();

    beforeLoading(fields);

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

  protected void beforeLoading(List<Field> fields) {
  }

  @Test
  void testAbsoluteDifferenceWithFirst() {
    AggregatedMeasure price = new AggregatedMeasure("price", "sum");
    ComparisonMeasure priceComp = QueryBuilder.bucketComparison(
            "priceDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            price,
            Map.of(
                    SCENARIO_FIELD_NAME, "first",
                    this.groupOfScenario, "g"
            ));
    AggregatedMeasure quantity = new AggregatedMeasure("quantity", "sum");
    ComparisonMeasure quantityComp = QueryBuilder.bucketComparison(
            "quantityDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            quantity,
            Map.of(
                    SCENARIO_FIELD_NAME, "first",
                    this.groupOfScenario, "g"
            ));

    var query = new QueryDto()
            .table(this.storeName)
            .withColumnSet(QueryDto.BUCKET, this.bucketCS)
            .withMeasure(priceComp)
            .withMeasure(price)
            .withMeasure(quantityComp)
            .withMeasure(quantity);

    Table dataset = this.executor.execute(query);
    Assertions.assertThat(dataset.headers().stream().map(Field::name)).containsExactly(
            this.groupOfScenario, SCENARIO_FIELD_NAME,
            "priceDiff", "sum(price)",
            "quantityDiff", "sum(quantity)");
    Assertions.assertThat(dataset).containsExactlyInAnyOrder(
            List.of("group1", "base", 0d, 15d, 0l, 34l),
            List.of("group1", "s1", 2d, 17d, -2l, 32l),
            List.of("group2", "base", 0d, 15d, 0l, 34l),
            List.of("group2", "s2", -0.5d, 14.5d, 1l, 35l),
            List.of("group3", "base", 0d, 15d, 0l, 34l),
            List.of("group3", "s1", 2d, 17d, -2l, 32l),
            List.of("group3", "s2", -0.5d, 14.5d, 1l, 35l));
  }

  @Test
  void testAbsoluteDifferenceWithPrevious() {
    AggregatedMeasure price = new AggregatedMeasure("price", "sum");
    ComparisonMeasure priceComp = QueryBuilder.bucketComparison(
            "priceDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            price,
            Map.of(
                    SCENARIO_FIELD_NAME, "s-1",
                    this.groupOfScenario, "g"
            ));
    AggregatedMeasure quantity = new AggregatedMeasure("quantity", "sum");
    ComparisonMeasure quantityComp = QueryBuilder.bucketComparison(
            "quantityDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            quantity,
            Map.of(
                    SCENARIO_FIELD_NAME, "s-1",
                    this.groupOfScenario, "g"
            ));

    var query = new QueryDto()
            .table(this.storeName)
            .withColumnSet(QueryDto.BUCKET, this.bucketCS)
            .withMeasure(priceComp)
            .withMeasure(price)
            .withMeasure(quantityComp)
            .withMeasure(quantity);

    Table dataset = this.executor.execute(query);
    Assertions.assertThat(dataset.headers().stream().map(Field::name)).containsExactly(
            this.groupOfScenario, SCENARIO_FIELD_NAME,
            "priceDiff", "sum(price)",
            "quantityDiff", "sum(quantity)");
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
    AggregatedMeasure price = new AggregatedMeasure("price", "sum");
    ComparisonMeasure priceComp = QueryBuilder.bucketComparison(
            "priceDiff",
            RELATIVE_DIFFERENCE,
            price,
            Map.of(
                    SCENARIO_FIELD_NAME, "first",
                    this.groupOfScenario, "g"
            ));
    AggregatedMeasure quantity = new AggregatedMeasure("quantity", "sum");
    ComparisonMeasure quantityComp = QueryBuilder.bucketComparison(
            "quantityDiff",
            RELATIVE_DIFFERENCE,
            quantity,
            Map.of(
                    SCENARIO_FIELD_NAME, "first",
                    this.groupOfScenario, "g"
            ));

    var query = new QueryDto()
            .table(this.storeName)
            .withColumnSet(QueryDto.BUCKET, this.bucketCS)
            .withMeasure(priceComp)
            .withMeasure(price)
            .withMeasure(quantityComp)
            .withMeasure(quantity);

    Table dataset = this.executor.execute(query);
    Assertions.assertThat(dataset.headers().stream().map(Field::name)).containsExactly(
            this.groupOfScenario, SCENARIO_FIELD_NAME,
            "priceDiff", "sum(price)",
            "quantityDiff", "sum(quantity)");
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
    BucketColumnSetDto bucketCS = new BucketColumnSetDto(this.groupOfScenario, SCENARIO_FIELD_NAME)
            .withNewBucket("B", List.of("s1", MAIN_SCENARIO_NAME))
            .withNewBucket("A", List.of("s2", MAIN_SCENARIO_NAME, "s1"))
            .withNewBucket("C", List.of(MAIN_SCENARIO_NAME, "s2", "s1"));

    var query = new QueryDto()
            .table(this.storeName)
            .withColumnSet(QueryDto.BUCKET, bucketCS)
            .withMeasure(CountMeasure.INSTANCE);

    Table dataset = this.executor.execute(query);
    Assertions.assertThat(dataset.headers().stream().map(Field::name))
            .containsExactly(this.groupOfScenario, SCENARIO_FIELD_NAME, CountMeasure.ALIAS);
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
}
