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

import static me.paulbares.query.QueryBuilder.*;
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
        SparkQueryControllerTest.assertMetadataResult(querier.metadata());
    }

    @Test
    void testRunQuery() {
        String url = "http://127.0.0.1:" + this.port;

        var querier = new HttpClientQuerier(url);

        QueryDto query = new QueryDto()
                .table("products")
                .wildcardCoordinate(SCENARIO_FIELD_NAME)
                .context(Totals.KEY, QueryBuilder.TOP)
                .aggregatedMeasure("marge", "sum");

        SimpleTable table = querier.run(query);
        SparkQueryControllerTest.assertQueryWithTotals(table);
    }

    @Test
    void testRunGroupingScenarioQuery() {
        String url = "http://127.0.0.1:" + this.port;

        var querier = new HttpClientQuerier(url);

        var query = scenarioComparisonQuery()
                .table("products")
                .defineNewGroup("group1", MAIN_SCENARIO_NAME, "mdd-baisse-simu-sensi")
                .defineNewGroup("group2", MAIN_SCENARIO_NAME, "mdd-baisse-simu-sensi", "mdd-baisse")
                .defineNewGroup("group3", MAIN_SCENARIO_NAME, "mdd-baisse");

        ScenarioComparisonDto comparison = comparison(
                "absolute_difference",
                aggregatedMeasure("marge", "sum"),
                true,
                "first");

        query.addScenarioComparison(comparison);

        SimpleTable table = querier.run(query);
        Assertions.assertThat(table.rows).containsExactlyInAnyOrder(
                List.of("group1", MAIN_SCENARIO_NAME, 0.0d, 280.00000000000006d),
                List.of("group1", "mdd-baisse-simu-sensi", -90.00000000000003d, 190.00000000000003d),
                List.of("group2", MAIN_SCENARIO_NAME, 0.0d, 280.00000000000006d),
                List.of("group2", "mdd-baisse-simu-sensi", -90.00000000000003d, 190.00000000000003d),
                List.of("group2", "mdd-baisse", -40.00000000000003d, 240.00000000000003d),
                List.of("group3", MAIN_SCENARIO_NAME, 0.0d, 280.00000000000006d),
                List.of("group3", "mdd-baisse", -40.00000000000003d, 240.00000000000003d)
        );
        Assertions.assertThat(table.columns)
                .containsExactly("group", SCENARIO_FIELD_NAME, "absolute_difference(sum(marge), first)", "sum(marge)");
    }

    @Test
    void testRunQueryWithCondition() {
        // Note. The CJ will make null appear in rows. We want to make sure null values are correctly handled.
        QueryDto query = new QueryDto()
                .table("products")
                .wildcardCoordinate(SCENARIO_FIELD_NAME)
                .wildcardCoordinate("type-marque")
                .condition(SCENARIO_FIELD_NAME, QueryBuilder.eq("base"))
                .context(Totals.KEY, QueryBuilder.TOP)
                .aggregatedMeasure("marge", "sum");

        String url = "http://127.0.0.1:" + this.port;

        var querier = new HttpClientQuerier(url);

        SimpleTable table = querier.run(query);
        Assertions.assertThat(table.rows).containsExactlyInAnyOrder(
                Arrays.asList(QueryEngine.GRAND_TOTAL, null, 280.00000000000006d),
                List.of(MAIN_SCENARIO_NAME, QueryEngine.TOTAL, 280.00000000000006d),
                List.of(MAIN_SCENARIO_NAME, "MDD", 190.00000000000003d),
                List.of(MAIN_SCENARIO_NAME, "MN", 90.00000000000003d)
        );
        Assertions.assertThat(table.columns).containsExactly(SCENARIO_FIELD_NAME, "type-marque", "sum(marge)");
    }
}
