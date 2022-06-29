package me.paulbares.query;

import me.paulbares.NewQueryExecutor;
import me.paulbares.query.comp.BinaryOperations;
import me.paulbares.query.dto.BucketColumnSetDto;
import me.paulbares.query.dto.NewQueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;
import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestScenarioGroupingExecutor {

  protected NewQueryExecutor executor;
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
  protected BucketColumnSetDto bucketCS = new BucketColumnSetDto(groupOfScenario, SCENARIO_FIELD_NAME)
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
    this.executor = new NewQueryExecutor(queryEngine);
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

  @BeforeEach
  void beforeEach() {
//    this.executor.queryCache.cache.invalidateAll();
  }

  @Test
  void testAbsoluteDifferenceWithFirst() {
    AggregatedMeasure price = new AggregatedMeasure("price", "sum");
    BinaryOperationMeasure priceComp = new BinaryOperationMeasure(
            "priceDiff",
            BinaryOperations.ABS_DIFF,
            price,
            Map.of(
                    SCENARIO_FIELD_NAME, "first",
                    groupOfScenario, "g"
            ));
    AggregatedMeasure quantity = new AggregatedMeasure("quantity", "sum");
    BinaryOperationMeasure quantityComp = new BinaryOperationMeasure(
            "quantityDiff",
            BinaryOperations.ABS_DIFF,
            quantity,
            Map.of(
                    SCENARIO_FIELD_NAME, "first",
                    groupOfScenario, "g"
            ));

    var query = new NewQueryDto()
            .table(this.storeName)
            .withColumnSet(NewQueryDto.BUCKET, bucketCS)
            .withMetric(priceComp)
            .withMetric(price)
            .withMetric(quantityComp)
            .withMetric(quantity);

    Table dataset = this.executor.execute(query);
    Assertions.assertThat(dataset.headers().stream().map(Field::name)).containsExactly(
            groupOfScenario, SCENARIO_FIELD_NAME,
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
    BinaryOperationMeasure priceComp = new BinaryOperationMeasure(
            "priceDiff",
            BinaryOperations.ABS_DIFF,
            price,
            Map.of(
                    SCENARIO_FIELD_NAME, "s-1",
                    groupOfScenario, "g"
            ));
    AggregatedMeasure quantity = new AggregatedMeasure("quantity", "sum");
    BinaryOperationMeasure quantityComp = new BinaryOperationMeasure(
            "quantityDiff",
            BinaryOperations.ABS_DIFF,
            quantity,
            Map.of(
                    SCENARIO_FIELD_NAME, "s-1",
                    groupOfScenario, "g"
            ));

    var query = new NewQueryDto()
            .table(this.storeName)
            .withColumnSet(NewQueryDto.BUCKET, bucketCS)
            .withMetric(priceComp)
            .withMetric(price)
            .withMetric(quantityComp)
            .withMetric(quantity);

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
            List.of("group3", "s2", -2.5, 14.5d, 3l, 35l));
  }

  @Test
  void testRelativeDifferenceWithFirst() {
    AggregatedMeasure price = new AggregatedMeasure("price", "sum");
    BinaryOperationMeasure priceComp = new BinaryOperationMeasure(
            "priceDiff",
            BinaryOperations.REL_DIFF,
            price,
            Map.of(
                    SCENARIO_FIELD_NAME, "first",
                    groupOfScenario, "g"
            ));
    AggregatedMeasure quantity = new AggregatedMeasure("quantity", "sum");
    BinaryOperationMeasure quantityComp = new BinaryOperationMeasure(
            "quantityDiff",
            BinaryOperations.REL_DIFF,
            quantity,
            Map.of(
                    SCENARIO_FIELD_NAME, "first",
                    groupOfScenario, "g"
            ));

    var query = new NewQueryDto()
            .table(this.storeName)
            .withColumnSet(NewQueryDto.BUCKET, bucketCS)
            .withMetric(priceComp)
            .withMetric(price)
            .withMetric(quantityComp)
            .withMetric(quantity);

    Table dataset = this.executor.execute(query);
    Assertions.assertThat(dataset.headers().stream().map(Field::name)).containsExactly(
            this.groupOfScenario, SCENARIO_FIELD_NAME,
            "priceDiff", "sum(price)",
            "quantityDiff", "sum(quantity)");
    Assertions.assertThat(dataset).containsExactlyInAnyOrder(
            List.of("group1", "base", 0d, 15d, 0d, 34l),
            List.of("group1", "s1", 0.13333333333333333d, 17d, -0.058823529411764705d, 32l),
            List.of("group2", "base", 0d, 15d, 0d, 34l),
            List.of("group2", "s2", -0.03333333333333333d, 14.5d, 0.029411764705882353d, 35l),
            List.of("group3", "base", 0d, 15d, 0d, 34l),
            List.of("group3", "s1", 0.13333333333333333d, 17d, -0.058823529411764705d, 32l),
            List.of("group3", "s2", -0.03333333333333333d, 14.5d, 0.029411764705882353d, 35l));
  }

//  @Test
//  @Disabled
//  void testCache() {
//    // Use own executor because stats in cache cannot be reset.
//    var executor = new ScenarioGroupingExecutor(createQueryEngine(this.datastore));
//
//    Map<String, List<String>> groups = new LinkedHashMap<>();
//    groups.put("group1", List.of("base", "s1"));
//    groups.put("group2", List.of("base"));
//
//    ScenarioGroupingQueryDto query = new ScenarioGroupingQueryDto()
//            .table(this.storeName)
//            .addScenarioComparison(new ScenarioComparisonDto(ABS_DIFF,
//                    new AggregatedMeasure("price", "sum"), true, ScenarioGroupingExecutor.REF_POS_FIRST))
//            .addScenarioComparison(new ScenarioComparisonDto(ABS_DIFF,
//                    new AggregatedMeasure("quantity", "sum"), true, ScenarioGroupingExecutor.REF_POS_FIRST))
//            .groups(groups);
//
//    // Should put the result in cache the first time.
//    int n = 10;
//    for (int i = 0; i < n; i++) {
//      executor.execute(query);
//    }
//
//    int missCount = 2;
//    int loadSuccessCount = 2;
//    int evictionCount = 0;
//    // Change the query. Remove a measure
//    query = new ScenarioGroupingQueryDto()
//            .table(this.storeName)
//            .addScenarioComparison(new ScenarioComparisonDto(ABS_DIFF,
//                    new AggregatedMeasure("price", "sum"), true, ScenarioGroupingExecutor.REF_POS_FIRST))
//            .groups(groups);
//    executor.execute(query);
//    assertCacheStats(executor.queryCache, n - 1, missCount, loadSuccessCount, evictionCount);
//
//    // Change bucketing but set of groups is the same
//    {
//      Map<String, List<String>> newGroups = new LinkedHashMap<>();
//      newGroups.put("newGroup", List.of("base", "s1"));
//      query.groups(newGroups);
//      executor.execute(query); // the result should come from the cache because sets of scenarios is the same
//      assertCacheStats(executor.queryCache, n, missCount, loadSuccessCount, evictionCount);
//    }
//
//    // Scenario removal
//    {
//      Map<String, List<String>> newGroups = new LinkedHashMap<>();
//      newGroups.put("newGroup", List.of("base")); // s1 is no more here. Should not hit the cache
//      query.groups(newGroups);
//      executor.execute(query); // the result should come from the cache because sets of scenarios is the same
//      assertCacheStats(executor.queryCache, n, ++missCount, ++loadSuccessCount, evictionCount);
//    }
//
//    // Scenario addition
//    {
//      Map<String, List<String>> newGroups = new LinkedHashMap<>();
//      newGroups.put("newGroup", List.of("base", "s1", "s2")); // s2 is added. should not hit the cache
//      query.groups(newGroups);
//      executor.execute(query); // the result should come from the cache because sets of scenarios is the same
//      assertCacheStats(executor.queryCache, n, ++missCount, ++loadSuccessCount, evictionCount);
//    }
//  }
//
//  protected void assertCacheStats(ScenarioGroupingCache cache,
//                                  int hitCount,
//                                  int missCount,
//                                  int loadSuccessCount,
//                                  int evictionCount) {
//    CacheStats stats = cache.cache.stats();
//    Assertions.assertThat(stats.hitCount()).isEqualTo(hitCount);
//    Assertions.assertThat(stats.missCount()).isEqualTo(missCount);
//    Assertions.assertThat(stats.loadSuccessCount()).isEqualTo(loadSuccessCount);
//    Assertions.assertThat(stats.evictionCount()).isEqualTo(evictionCount);
//  }
}
