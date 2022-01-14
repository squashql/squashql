package me.paulbares;

import me.paulbares.query.ComparisonMethod;
import me.paulbares.query.spark.SparkQueryEngine;
import me.paulbares.query.ScenarioGroupingQuery;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static me.paulbares.SparkDatastore.MAIN_SCENARIO_NAME;

public class TestQueryEngineGrouping {

  static SparkDatastore ds;

  @BeforeAll
  static void setup() {
    Field ean = new Field("ean", String.class);
    Field category = new Field("category", String.class);
    Field price = new Field("price", double.class);
    Field qty = new Field("quantity", int.class);
    ds = new SparkDatastore(List.of(ean, category, price, qty));

    ds.load(MAIN_SCENARIO_NAME, List.of(
            new Object[]{"bottle", "drink", 2d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    ds.load("s1", List.of(
            new Object[]{"bottle", "drink", 4d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    ds.load("s2", List.of(
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
      Dataset<Row> dataset = new SparkQueryEngine(ds).executeGrouping(query.comparisonMethod(ComparisonMethod.ABSOLUTE));
      Assertions.assertThat(dataset.columns()).containsExactly(
              "group", "scenario", "abs. diff. sum(price)", "abs. diff. sum(quantity)");

      List<Row> collect = dataset.collectAsList();
      Assertions.assertThat(collect).containsExactly(
              RowFactory.create("group1", "base", 0d, 0),
              RowFactory.create("group1", "s1", 2d, 0),
              RowFactory.create("group2", "base", 0d, 0),
              RowFactory.create("group2", "s2", -0.5d, 0),
              RowFactory.create("group3", "base", 0d, 0),
              RowFactory.create("group3", "s1", 2d, 0),
              RowFactory.create("group3", "s2", -2.5d, 0));
    }

    { // RELATIVE
      Dataset<Row> dataset = new SparkQueryEngine(ds).executeGrouping(query.comparisonMethod(ComparisonMethod.RELATIVE));
      Assertions.assertThat(dataset.columns()).containsExactly(
              "group", "scenario", "rel. diff. sum(price)", "rel. diff. sum(quantity)");

      List<Row> collect = dataset.collectAsList();
      Assertions.assertThat(collect).containsExactly(
              RowFactory.create("group1", "base", 0d, 0),
              RowFactory.create("group1", "s1", 0.13333333333333333d, 0),
              RowFactory.create("group2", "base", 0d, 0),
              RowFactory.create("group2", "s2", -0.03333333333333333, 0),
              RowFactory.create("group3", "base", 0d, 0),
              RowFactory.create("group3", "s1", 0.13333333333333333, 0),
              RowFactory.create("group3", "s2", -0.14705882352941177, 0));
    }
  }
}
