package me.paulbares.query;

import com.google.common.cache.CacheStats;
import me.paulbares.query.dto.ScenarioComparisonDto;
import me.paulbares.query.dto.ScenarioGroupingQueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
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

  protected ScenarioGroupingExecutor executor;
  protected Datastore datastore;
  protected String storeName = "storeName";

  protected abstract QueryEngine createQueryEngine(Datastore datastore);

  protected abstract Datastore createDatastore(String storeName, List<Field> fields);

  protected Map<String, List<String>> groups = new LinkedHashMap<>();

  {
    this.groups.put("group1", List.of("base", "s1"));
    this.groups.put("group2", List.of("base", "s2"));
    this.groups.put("group3", List.of("base", "s1", "s2"));
  }

  @BeforeAll
  void setup() {
    Field ean = new Field("ean", String.class);
    Field category = new Field("category", String.class);
    Field price = new Field("price", double.class);
    Field qty = new Field("quantity", int.class);

    this.datastore = createDatastore(this.storeName, List.of(ean, category, price, qty));
    QueryEngine queryEngine = createQueryEngine(this.datastore);
    this.executor = new ScenarioGroupingExecutor(queryEngine);

    this.datastore.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"bottle", "drink", 2d, 11},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    this.datastore.load("s1", this.storeName, List.of(
            new Object[]{"bottle", "drink", 4d, 9},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    this.datastore.load("s2", this.storeName, List.of(
            new Object[]{"bottle", "drink", 1.5d, 12},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));
  }

  @BeforeEach
  void beforeEach() {
    this.executor.queryCache.cache.invalidateAll();
  }

  @Test
  void testAbsoluteDifferenceWithFirst() {
    ScenarioGroupingQueryDto query = new ScenarioGroupingQueryDto()
            .table(this.storeName)
            .addScenarioComparison(new ScenarioComparisonDto(ScenarioGroupingExecutor.COMPARISON_METHOD_ABS_DIFF,
                    new AggregatedMeasure("price", "sum"), true, ScenarioGroupingExecutor.REF_POS_FIRST))
            .addScenarioComparison(new ScenarioComparisonDto(ScenarioGroupingExecutor.COMPARISON_METHOD_ABS_DIFF,
                    new AggregatedMeasure("quantity", "sum"), true, ScenarioGroupingExecutor.REF_POS_FIRST))
            .groups(this.groups);

    Table dataset = this.executor.execute(query);
    Assertions.assertThat(dataset.headers().stream().map(Field::name)).containsExactly(
            "group", SCENARIO_FIELD_NAME,
            "absolute_difference(sum(price), first)", "sum(price)",
            "absolute_difference(sum(quantity), first)", "sum(quantity)");
    Assertions.assertThat(dataset).containsExactly(
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
    ScenarioGroupingQueryDto query = new ScenarioGroupingQueryDto()
            .table(this.storeName)
            .addScenarioComparison(new ScenarioComparisonDto(ScenarioGroupingExecutor.COMPARISON_METHOD_ABS_DIFF,
                    new AggregatedMeasure("price", "sum"), true, ScenarioGroupingExecutor.REF_POS_PREVIOUS))
            .addScenarioComparison(new ScenarioComparisonDto(ScenarioGroupingExecutor.COMPARISON_METHOD_ABS_DIFF,
                    new AggregatedMeasure("quantity", "sum"), true, ScenarioGroupingExecutor.REF_POS_PREVIOUS))
            .groups(this.groups);

    Table dataset = this.executor.execute(query);
    Assertions.assertThat(dataset.headers().stream().map(Field::name)).containsExactly(
            "group", SCENARIO_FIELD_NAME,
            "absolute_difference(sum(price), previous)", "sum(price)",
            "absolute_difference(sum(quantity), previous)", "sum(quantity)");
    Assertions.assertThat(dataset).containsExactly(
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
    ScenarioGroupingQueryDto query = new ScenarioGroupingQueryDto()
            .table(this.storeName)
            .addScenarioComparison(new ScenarioComparisonDto(ScenarioGroupingExecutor.COMPARISON_METHOD_REL_DIFF,
                    new AggregatedMeasure("price", "sum"), true, ScenarioGroupingExecutor.REF_POS_FIRST))
            .addScenarioComparison(new ScenarioComparisonDto(ScenarioGroupingExecutor.COMPARISON_METHOD_REL_DIFF,
                    new AggregatedMeasure("quantity", "sum"), true, ScenarioGroupingExecutor.REF_POS_FIRST))
            .groups(this.groups);

    Table dataset = this.executor.execute(query);
    Assertions.assertThat(dataset.headers().stream().map(Field::name)).containsExactly(
            "group", SCENARIO_FIELD_NAME,
            "relative_difference(sum(price), first)", "sum(price)",
            "relative_difference(sum(quantity), first)", "sum(quantity)");
    Assertions.assertThat(dataset).containsExactly(
            List.of("group1", "base", 0d, 15d, 0d, 34l),
            List.of("group1", "s1", 0.13333333333333333d, 17d, -0.058823529411764705d, 32l),
            List.of("group2", "base", 0d, 15d, 0d, 34l),
            List.of("group2", "s2", -0.03333333333333333d, 14.5d, 0.029411764705882353d, 35l),
            List.of("group3", "base", 0d, 15d, 0d, 34l),
            List.of("group3", "s1", 0.13333333333333333d, 17d, -0.058823529411764705d, 32l),
            List.of("group3", "s2", -0.03333333333333333d, 14.5d, 0.029411764705882353d, 35l));
  }

  @Test
  void testRelativeDifferenceWithFirstWithLabels() {
    ScenarioGroupingQueryDto query = new ScenarioGroupingQueryDto()
            .table(this.storeName)
            .addScenarioComparison(new ScenarioComparisonDto(ScenarioGroupingExecutor.COMPARISON_METHOD_REL_DIFF,
                    new AggregatedMeasure("price", "sum"), true, ScenarioGroupingExecutor.REF_POS_FIRST, "label1"))
            .addScenarioComparison(new ScenarioComparisonDto(ScenarioGroupingExecutor.COMPARISON_METHOD_REL_DIFF,
                    new AggregatedMeasure("quantity", "sum"), true, ScenarioGroupingExecutor.REF_POS_FIRST, "label2"))
            .groups(this.groups);

    Table dataset = this.executor.execute(query);
    Assertions.assertThat(dataset.headers().stream().map(Field::name)).containsExactly(
            "group", SCENARIO_FIELD_NAME, "label1", "sum(price)", "label2", "sum(quantity)");
    Assertions.assertThat(dataset).containsExactly(
            List.of("group1", "base", 0d, 15d, 0d, 34l),
            List.of("group1", "s1", 0.13333333333333333d, 17d, -0.058823529411764705d, 32l),
            List.of("group2", "base", 0d, 15d, 0d, 34l),
            List.of("group2", "s2", -0.03333333333333333d, 14.5d, 0.029411764705882353d, 35l),
            List.of("group3", "base", 0d, 15d, 0d, 34l),
            List.of("group3", "s1", 0.13333333333333333d, 17d, -0.058823529411764705d, 32l),
            List.of("group3", "s2", -0.03333333333333333d, 14.5d, 0.029411764705882353d, 35l));
  }

  @Test
  void testRelativeDifferenceWithPrevious() {
    ScenarioGroupingQueryDto query = new ScenarioGroupingQueryDto()
            .table(this.storeName)
            .addScenarioComparison(new ScenarioComparisonDto(ScenarioGroupingExecutor.COMPARISON_METHOD_REL_DIFF,
                    new AggregatedMeasure("price", "sum"), true, ScenarioGroupingExecutor.REF_POS_PREVIOUS))
            .addScenarioComparison(new ScenarioComparisonDto(ScenarioGroupingExecutor.COMPARISON_METHOD_REL_DIFF,
                    new AggregatedMeasure("quantity", "sum"), true, ScenarioGroupingExecutor.REF_POS_PREVIOUS))
            .groups(this.groups);

    Table dataset = this.executor.execute(query);
    Assertions.assertThat(dataset.headers().stream().map(Field::name)).containsExactly(
            "group", SCENARIO_FIELD_NAME,
            "relative_difference(sum(price), previous)", "sum(price)",
            "relative_difference(sum(quantity), previous)", "sum(quantity)");
    Assertions.assertThat(dataset).containsExactly(
            List.of("group1", "base", 0d, 15d, 0d, 34l),
            List.of("group1", "s1", 0.13333333333333333d, 17d, -0.058823529411764705d, 32l),
            List.of("group2", "base", 0d, 15d, 0d, 34l),
            List.of("group2", "s2", -0.03333333333333333d, 14.5d, 0.029411764705882353d, 35l),
            List.of("group3", "base", 0d, 15d, 0d, 34l),
            List.of("group3", "s1", 0.13333333333333333d, 17d, -0.058823529411764705d, 32l),
            List.of("group3", "s2", -0.14705882352941177d, 14.5d, 0.09375d, 35l));
  }

  @Test
  void testShowValueOff() {
    ScenarioGroupingQueryDto query = new ScenarioGroupingQueryDto()
            .table(this.storeName)
            .addScenarioComparison(new ScenarioComparisonDto(ScenarioGroupingExecutor.COMPARISON_METHOD_ABS_DIFF,
                    new AggregatedMeasure("price", "sum"), false, ScenarioGroupingExecutor.REF_POS_FIRST))
            .addScenarioComparison(new ScenarioComparisonDto(ScenarioGroupingExecutor.COMPARISON_METHOD_ABS_DIFF,
                    new AggregatedMeasure("quantity", "sum"), true, ScenarioGroupingExecutor.REF_POS_FIRST))
            .groups(this.groups);

    Table dataset = this.executor.execute(query);
    Assertions.assertThat(dataset.headers().stream().map(Field::name)).containsExactly(
            "group", SCENARIO_FIELD_NAME,
            "absolute_difference(sum(price), first)",
            "absolute_difference(sum(quantity), first)", "sum(quantity)");
    Assertions.assertThat(dataset).containsExactly(
            List.of("group1", "base", 0d, 0l, 34l),
            List.of("group1", "s1", 2d, -2l, 32l),
            List.of("group2", "base", 0d, 0l, 34l),
            List.of("group2", "s2", -0.5d, 1l, 35l),
            List.of("group3", "base", 0d, 0l, 34l),
            List.of("group3", "s1", 2d, -2l, 32l),
            List.of("group3", "s2", -0.5d, 1l, 35l));
  }

  @Test
  void testCache() {
    // Use own executor because stats in cache cannot be reset.
    var executor = new ScenarioGroupingExecutor(createQueryEngine(this.datastore));

    Map<String, List<String>> groups = new LinkedHashMap<>();
    groups.put("group1", List.of("base", "s1"));
    groups.put("group2", List.of("base"));

    ScenarioGroupingQueryDto query = new ScenarioGroupingQueryDto()
            .table(this.storeName)
            .addScenarioComparison(new ScenarioComparisonDto(ScenarioGroupingExecutor.COMPARISON_METHOD_ABS_DIFF,
                    new AggregatedMeasure("price", "sum"), true, ScenarioGroupingExecutor.REF_POS_FIRST))
            .addScenarioComparison(new ScenarioComparisonDto(ScenarioGroupingExecutor.COMPARISON_METHOD_ABS_DIFF,
                    new AggregatedMeasure("quantity", "sum"), true, ScenarioGroupingExecutor.REF_POS_FIRST))
            .groups(groups);

    // Should put the result in cache the first time.
    int n = 10;
    for (int i = 0; i < n; i++) {
      executor.execute(query);
    }

    int missCount = 2;
    int loadSuccessCount = 2;
    int evictionCount = 0;
    // Change the query. Remove a measure
    query = new ScenarioGroupingQueryDto()
            .table(this.storeName)
            .addScenarioComparison(new ScenarioComparisonDto(ScenarioGroupingExecutor.COMPARISON_METHOD_ABS_DIFF,
                    new AggregatedMeasure("price", "sum"), true, ScenarioGroupingExecutor.REF_POS_FIRST))
            .groups(groups);
    executor.execute(query);
    assertCacheStats(executor.queryCache, n - 1, missCount, loadSuccessCount, evictionCount);

    // Change bucketing but set of groups is the same
    {
      Map<String, List<String>> newGroups = new LinkedHashMap<>();
      newGroups.put("newGroup", List.of("base", "s1"));
      query.groups(newGroups);
      executor.execute(query); // the result should come from the cache because sets of scenarios is the same
      assertCacheStats(executor.queryCache, n, missCount, loadSuccessCount, evictionCount);
    }

    // Scenario removal
    {
      Map<String, List<String>> newGroups = new LinkedHashMap<>();
      newGroups.put("newGroup", List.of("base")); // s1 is no more here. Should not hit the cache
      query.groups(newGroups);
      executor.execute(query); // the result should come from the cache because sets of scenarios is the same
      assertCacheStats(executor.queryCache, n, ++missCount, ++loadSuccessCount, evictionCount);
    }

    // Scenario addition
    {
      Map<String, List<String>> newGroups = new LinkedHashMap<>();
      newGroups.put("newGroup", List.of("base", "s1", "s2")); // s2 is added. should not hit the cache
      query.groups(newGroups);
      executor.execute(query); // the result should come from the cache because sets of scenarios is the same
      assertCacheStats(executor.queryCache, n, ++missCount, ++loadSuccessCount, evictionCount);
    }
  }

  protected void assertCacheStats(ScenarioGroupingCache cache,
                                  int hitCount,
                                  int missCount,
                                  int loadSuccessCount,
                                  int evictionCount) {
    CacheStats stats = cache.cache.stats();
    Assertions.assertThat(stats.hitCount()).isEqualTo(hitCount);
    Assertions.assertThat(stats.missCount()).isEqualTo(missCount);
    Assertions.assertThat(stats.loadSuccessCount()).isEqualTo(loadSuccessCount);
    Assertions.assertThat(stats.evictionCount()).isEqualTo(evictionCount);
  }
}
