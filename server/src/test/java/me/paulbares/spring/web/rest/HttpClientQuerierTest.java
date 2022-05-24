package me.paulbares.spring.web.rest;

import me.paulbares.client.HttpClientQuerier;
import me.paulbares.client.SimpleTable;
import me.paulbares.query.QueryBuilder;
import me.paulbares.query.QueryEngine;
import me.paulbares.query.context.Totals;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.ScenarioComparisonDto;
import org.apache.catalina.webresources.TomcatURLStreamHandlerFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;

import java.util.Arrays;
import java.util.List;

import static me.paulbares.query.QueryBuilder.aggregatedMeasure;
import static me.paulbares.query.QueryBuilder.comparison;
import static me.paulbares.query.QueryBuilder.scenarioComparisonQuery;
import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;
import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class HttpClientQuerierTest {

  static {
    // FIXME: why do I need to do this? (fails in maven build without it)
    // Fix found here https://github.com/spring-projects/spring-boot/issues/21535
    TomcatURLStreamHandlerFactory.disable();
  }

  @LocalServerPort
  int port;

  @Test
  void testGetMetadata() {
    String url = "http://127.0.0.1:" + this.port;

    var querier = new HttpClientQuerier(url);
    SparkQueryControllerTest.assertMetadataResult(querier.metadata(), false);
  }

  @Test
  void testRunQuery() {
    String url = "http://127.0.0.1:" + this.port;

    var querier = new HttpClientQuerier(url);

    QueryDto query = new QueryDto()
            .table("our_prices")
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .context(Totals.KEY, QueryBuilder.TOP)
            .aggregatedMeasure("quantity", "sum");

    SimpleTable table = querier.run(query);
    SparkQueryControllerTest.assertQueryWithTotals(table);
  }

  @Test
  void testRunGroupingScenarioQuery() {
    String url = "http://127.0.0.1:" + this.port;

    var querier = new HttpClientQuerier(url);

    var query = scenarioComparisonQuery()
            .table("our_prices")
            .defineNewGroup("group1", MAIN_SCENARIO_NAME, "MN up")
            .defineNewGroup("group2", MAIN_SCENARIO_NAME, "MN & MDD up")
            .defineNewGroup("group3", MAIN_SCENARIO_NAME, "MN up", "MN & MDD up");

    ScenarioComparisonDto comparison = comparison(
            "absolute_difference",
            aggregatedMeasure("capdv", "sum"),
            true,
            "first");

    query.addScenarioComparison(comparison);

    SimpleTable table = querier.run(query);
    double baseValue = 40_000d;
    double mnValue = 42_000d;
    double mnmddValue = 44_000d;
    Assertions.assertThat(table.rows).containsExactlyInAnyOrder(
            List.of("group1", MAIN_SCENARIO_NAME, 0d, baseValue),
            List.of("group1", "MN up", mnValue - baseValue, mnValue),
            List.of("group2", MAIN_SCENARIO_NAME, 0d, baseValue),
            List.of("group2", "MN & MDD up", mnmddValue - baseValue, mnmddValue),
            List.of("group3", MAIN_SCENARIO_NAME, 0d, baseValue),
            List.of("group3", "MN up", mnValue - baseValue, mnValue),
            List.of("group3", "MN & MDD up", mnmddValue - baseValue, mnmddValue));
    Assertions.assertThat(table.columns)
            .containsExactly("group", SCENARIO_FIELD_NAME, "absolute_difference(sum(capdv), first)", "sum(capdv)");
  }

  @Test
  void testRunQueryWithCondition() {
    // Note. The CJ will make null appear in rows. We want to make sure null values are correctly handled.
    QueryDto query = new QueryDto()
            .table("our_prices")
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .wildcardCoordinate("pdv")
            .condition(SCENARIO_FIELD_NAME, QueryBuilder.eq(MAIN_SCENARIO_NAME))
            .context(Totals.KEY, QueryBuilder.TOP)
            .aggregatedMeasure("price", "sum");

    String url = "http://127.0.0.1:" + this.port;

    var querier = new HttpClientQuerier(url);

    SimpleTable table = querier.run(query);
    Assertions.assertThat(table.rows).containsExactlyInAnyOrder(
            Arrays.asList(QueryEngine.GRAND_TOTAL, null, 40d),
            List.of(MAIN_SCENARIO_NAME, QueryEngine.TOTAL, 40d),
            List.of(MAIN_SCENARIO_NAME, "ITM Balma", 20d),
            List.of(MAIN_SCENARIO_NAME, "ITM Toulouse and Drive", 20d)
    );
    Assertions.assertThat(table.columns).containsExactly(SCENARIO_FIELD_NAME, "pdv", "sum(price)");
  }
}
