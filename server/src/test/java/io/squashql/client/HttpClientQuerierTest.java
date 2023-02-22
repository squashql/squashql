package io.squashql.client;

import io.squashql.client.http.HttpClientQuerier;
import io.squashql.query.*;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.*;
import io.squashql.spring.SquashQLApplication;
import io.squashql.spring.dataset.DatasetTestConfig;
import io.squashql.spring.web.rest.QueryControllerTest;
import org.apache.catalina.webresources.TomcatURLStreamHandlerFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.squashql.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static io.squashql.transaction.TransactionManager.SCENARIO_FIELD_NAME;

@SpringBootTest(
        classes = SquashQLApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.main.allow-bean-definition-overriding=true")
@Import(DatasetTestConfig.class)
public class HttpClientQuerierTest {

  static {
    // FIXME: why do I need to do this? (fails in maven build without it)
    // Fix found here https://github.com/spring-projects/spring-boot/issues/21535
    TomcatURLStreamHandlerFactory.disable();
  }

  @LocalServerPort
  int port;

  String url() {
    return "http://127.0.0.1:" + this.port;
  }

  @Test
  void testGetMetadata() {
    var querier = new HttpClientQuerier(url());
    QueryControllerTest.assertMetadataResult(querier.metadata());
  }

  @Test
  void testRunQuery() {
    var querier = new HttpClientQuerier(url());

    QueryDto query = new QueryDto()
            .table("our_prices")
            .withColumn(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("qs", "quantity", "sum");

    QueryResultDto response = querier.run(query);
    assertQuery(response.table);
    Assertions.assertThat(response.metadata).containsExactly(
            new MetadataItem(SCENARIO_FIELD_NAME, SCENARIO_FIELD_NAME, String.class),
            new MetadataItem("qs", "sum(quantity)", long.class));

    Assertions.assertThat(response.debug.cache).isNotNull();
    Assertions.assertThat(response.debug.timings).isNotNull();
  }

  @Test
  void testMergeQuery() {
    var querier = new HttpClientQuerier(url());

    QueryDto query1 = new QueryDto()
            .table("our_prices")
            .withColumn(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("qs", "quantity", "sum");
    QueryDto query2 = new QueryDto()
            .table("our_prices")
            .withColumn(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("qa", "quantity", "avg");

    QueryResultDto response = querier.queryMerge(new QueryMergeDto(query1, query2));
    Assertions.assertThat(response.table.rows).containsExactlyInAnyOrder(List.of("MDD up", 4000, 1000d),
            List.of("MN & MDD down", 4000, 1000d),
            List.of("MN & MDD up", 4000, 1000d),
            List.of("MN up", 4000, 1000d),
            List.of(MAIN_SCENARIO_NAME, 4000, 1000d));
    Assertions.assertThat(response.table.columns).containsExactly(SCENARIO_FIELD_NAME, "qs", "qa");
    Assertions.assertThat(response.metadata).containsExactly(
            new MetadataItem(SCENARIO_FIELD_NAME, SCENARIO_FIELD_NAME, String.class),
            new MetadataItem("qs", "sum(quantity)", long.class),
            new MetadataItem("qa", "avg(quantity)", double.class));

    Assertions.assertThat(response.debug).isNull();
  }

  @Test
  void testRunGroupingScenarioQuery() {
    var querier = new HttpClientQuerier(url());

    BucketColumnSetDto bucketCS = new BucketColumnSetDto("group", SCENARIO_FIELD_NAME)
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "MN up"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "MN & MDD up"))
            .withNewBucket("group3", List.of(MAIN_SCENARIO_NAME, "MN up", "MN & MDD up"));

    Measure aggregatedMeasure = Functions.sum("capdv", "capdv");
    ComparisonMeasureReferencePosition capdvDiff = new ComparisonMeasureReferencePosition(
            "capdvDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            aggregatedMeasure,
            Map.of(
                    SCENARIO_FIELD_NAME, "first",
                    "group", "g"
            ),
            ColumnSetKey.BUCKET);
    var query = Query
            .from("our_prices")
            .select_(List.of(bucketCS), List.of(capdvDiff, aggregatedMeasure))
            .build();

    QueryResultDto response = querier.run(query);
    SimpleTableDto table = response.table;
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
            .containsExactly("group", SCENARIO_FIELD_NAME, "capdvDiff", "capdv");
  }

  @Test
  void testRunQueryWithCondition() {
    // Note. The CJ will make null appear in rows. We want to make sure null values are correctly handled.
    QueryDto query = Query
            .from("our_prices")
            .where(SCENARIO_FIELD_NAME, Functions.eq(MAIN_SCENARIO_NAME))
            .select(List.of(SCENARIO_FIELD_NAME, "pdv"), List.of(Functions.sum("ps", "price")))
            .build();

    var querier = new HttpClientQuerier(url());

    QueryResultDto response = querier.run(query);
    SimpleTableDto table = response.table;
    Assertions.assertThat(table.rows).containsExactlyInAnyOrder(
            List.of(MAIN_SCENARIO_NAME, "ITM Balma", 20d),
            List.of(MAIN_SCENARIO_NAME, "ITM Toulouse and Drive", 20d)
    );
    Assertions.assertThat(table.columns).containsExactly(SCENARIO_FIELD_NAME, "pdv", "ps");
  }

  static void assertQuery(SimpleTableDto table) {
    Assertions.assertThat(table.rows).containsExactlyInAnyOrder(
            List.of("MDD up", 4000),
            List.of("MN & MDD down", 4000),
            List.of("MN & MDD up", 4000),
            List.of("MN up", 4000),
            List.of(MAIN_SCENARIO_NAME, 4000));
    Assertions.assertThat(table.columns).containsExactly(SCENARIO_FIELD_NAME, "qs");
  }

  @Test
  void testSetExpressions() {
    AggregatedMeasure a = new AggregatedMeasure("a", "a", "sum");
    AggregatedMeasure b = new AggregatedMeasure("b", "b", "sum");
    Measure plus = Functions.plus("a+b", a, b);

    List<Measure> input = Stream.of(a, b, plus).map(m -> m.withExpression(null)).toList(); // Expression should not be defined but computed and set by the server

    var querier = new HttpClientQuerier(url());

    List<Measure> expression = querier.expression(input);
    Assertions.assertThat(expression.stream().map(Measure::expression)).containsExactly("sum(a)", "sum(b)", "a + b");
  }
}
