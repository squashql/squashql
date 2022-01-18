package me.paulbares.query;

import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestScenarioGroupingExecutor {

  protected ScenarioGroupingExecutor executor;

  protected abstract QueryEngine createQueryEngine(Datastore datastore);

  protected abstract Datastore createDatastore(List<Field> fields);

  @BeforeAll
  void setup() {
    Field ean = new Field("ean", String.class);
    Field category = new Field("category", String.class);
    Field price = new Field("price", double.class);
    Field qty = new Field("quantity", int.class);

    Datastore datastore = createDatastore(List.of(ean, category, price, qty));
    QueryEngine queryEngine = createQueryEngine(datastore);
    this.executor = new ScenarioGroupingExecutor(queryEngine);

    datastore.load(MAIN_SCENARIO_NAME, List.of(
            new Object[]{"bottle", "drink", 2d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    datastore.load("s1", List.of(
            new Object[]{"bottle", "drink", 4d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    datastore.load("s2", List.of(
            new Object[]{"bottle", "drink", 1.5d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));
  }

  @Test
  void testQueryGroupsOfScenario() {
    Map<String, List<String>> groups = new LinkedHashMap<>();
    groups.put("group1", List.of("base", "s1"));
    groups.put("group2", List.of("base", "s2"));
    groups.put("group3", List.of("base", "s1", "s2"));

    ScenarioGroupingQuery query = new ScenarioGroupingQuery()
            .addAggregatedMeasure("price", "sum")
            .addAggregatedMeasure("quantity", "sum")
            .groups(groups);

    { // ABSOLUTE
      Table dataset = executor.execute(query.comparisonMethod(ComparisonMethod.ABSOLUTE));
      Assertions.assertThat(dataset.fields().stream().map(Field::name)).containsExactly(
              "group", "scenario", "abs. diff. sum(price)", "abs. diff. sum(quantity)");

      Assertions.assertThat(dataset).containsExactly(
              List.of("group1", "base", 0d, 0l),
              List.of("group1", "s1", 2d, 0l),
              List.of("group2", "base", 0d, 0l),
              List.of("group2", "s2", -0.5d, 0l),
              List.of("group3", "base", 0d, 0l),
              List.of("group3", "s1", 2d, 0l),
              List.of("group3", "s2", -2.5d, 0l));
    }

    { // RELATIVE
      Table dataset = executor.execute(query.comparisonMethod(ComparisonMethod.RELATIVE));
      Assertions.assertThat(dataset.fields().stream().map(Field::name)).containsExactly(
              "group", "scenario", "rel. diff. sum(price)", "rel. diff. sum(quantity)");

      Assertions.assertThat(dataset).containsExactly(
              List.of("group1", "base", 0d, 0l),
              List.of("group1", "s1", 0.13333333333333333d, 0l),
              List.of("group2", "base", 0d, 0l),
              List.of("group2", "s2", -0.03333333333333333, 0l),
              List.of("group3", "base", 0d, 0l),
              List.of("group3", "s1", 0.13333333333333333, 0l),
              List.of("group3", "s2", -0.14705882352941177, 0l));
    }
  }
}
