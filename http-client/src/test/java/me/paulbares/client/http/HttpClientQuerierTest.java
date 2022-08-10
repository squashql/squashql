package me.paulbares.client.http;

import me.paulbares.AitmApplication;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.AggregatedMeasure;
import me.paulbares.query.ComparisonMeasure;
import me.paulbares.query.ComparisonMethod;
import me.paulbares.query.QueryBuilder;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.BucketColumnSetDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.spring.web.rest.QueryControllerTest;
import me.paulbares.transaction.TransactionManager;
import org.apache.catalina.webresources.TomcatURLStreamHandlerFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static me.paulbares.transaction.TransactionManager.SCENARIO_FIELD_NAME;

@SpringBootTest(
        classes = AitmApplication.class,
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

  @Test
  void testGetMetadata() {
    String url = "http://127.0.0.1:" + this.port;

    var querier = new HttpClientQuerier(url);
    QueryControllerTest.assertMetadataResult(querier.metadata(), false);
  }

  @Test
  void testRunQuery() {
    String url = "http://127.0.0.1:" + this.port;

    var querier = new HttpClientQuerier(url);

    QueryDto query = new QueryDto()
            .table("our_prices")
            .withColumn(TransactionManager.SCENARIO_FIELD_NAME)
            .aggregatedMeasure("quantity", "sum");

    Map<String, Object> response = (Map<String, Object>) querier.run(query);
    assertQuery(JacksonUtil.deserialize((String) response.get("table"), SimpleTable.class),false);
  }

  @Test
  void testRunGroupingScenarioQuery() {
    String url = "http://127.0.0.1:" + this.port;

    var querier = new HttpClientQuerier(url);

    BucketColumnSetDto bucketCS = new BucketColumnSetDto("group", TransactionManager.SCENARIO_FIELD_NAME)
            .withNewBucket("group1", List.of(TransactionManager.MAIN_SCENARIO_NAME, "MN up"))
            .withNewBucket("group2", List.of(TransactionManager.MAIN_SCENARIO_NAME, "MN & MDD up"))
            .withNewBucket("group3", List.of(TransactionManager.MAIN_SCENARIO_NAME, "MN up", "MN & MDD up"));

    AggregatedMeasure aggregatedMeasure = new AggregatedMeasure("capdv", "sum");
    ComparisonMeasure capdvDiff = QueryBuilder.bucketComparison(
            "capdvDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            aggregatedMeasure,
            Map.of(
                    TransactionManager.SCENARIO_FIELD_NAME, "first",
                    "group", "g"
            ));
    var query = new QueryDto()
            .table("our_prices")
            .withColumnSet(QueryDto.BUCKET, bucketCS)
            .withMeasure(capdvDiff)
            .withMeasure(aggregatedMeasure);

    Map<String, Object> response = (Map<String, Object>) querier.run(query);
    SimpleTable table = JacksonUtil.deserialize((String) response.get("table"), SimpleTable.class);
    double baseValue = 40_000d;
    double mnValue = 42_000d;
    double mnmddValue = 44_000d;
    Assertions.assertThat(table.rows).containsExactlyInAnyOrder(
            List.of("group1", TransactionManager.MAIN_SCENARIO_NAME, 0d, baseValue),
            List.of("group1", "MN up", mnValue - baseValue, mnValue),
            List.of("group2", TransactionManager.MAIN_SCENARIO_NAME, 0d, baseValue),
            List.of("group2", "MN & MDD up", mnmddValue - baseValue, mnmddValue),
            List.of("group3", TransactionManager.MAIN_SCENARIO_NAME, 0d, baseValue),
            List.of("group3", "MN up", mnValue - baseValue, mnValue),
            List.of("group3", "MN & MDD up", mnmddValue - baseValue, mnmddValue));
    Assertions.assertThat(table.columns)
            .containsExactly("group", TransactionManager.SCENARIO_FIELD_NAME, "capdvDiff", "sum(capdv)");
  }

  @Test
  void testRunQueryWithCondition() {
    // Note. The CJ will make null appear in rows. We want to make sure null values are correctly handled.
    QueryDto query = new QueryDto()
            .table("our_prices")
            .withColumn(TransactionManager.SCENARIO_FIELD_NAME)
            .withColumn("pdv")
            .withCondition(TransactionManager.SCENARIO_FIELD_NAME, QueryBuilder.eq(TransactionManager.MAIN_SCENARIO_NAME))
            .aggregatedMeasure("price", "sum");

    String url = "http://127.0.0.1:" + this.port;

    var querier = new HttpClientQuerier(url);

    Map<String, Object> response = (Map<String, Object>) querier.run(query);
    SimpleTable table = JacksonUtil.deserialize((String) response.get("table"), SimpleTable.class);
    Assertions.assertThat(table.rows).containsExactlyInAnyOrder(
            List.of(TransactionManager.MAIN_SCENARIO_NAME, "ITM Balma", 20d),
            List.of(TransactionManager.MAIN_SCENARIO_NAME, "ITM Toulouse and Drive", 20d)
    );
    Assertions.assertThat(table.columns).containsExactly(TransactionManager.SCENARIO_FIELD_NAME, "pdv", "sum(price)");
  }

  static void assertQuery(SimpleTable table, boolean withTotals) {
    List[] lists = {List.of("MDD up", 4000),
            List.of("MN & MDD down", 4000),
            List.of("MN & MDD up", 4000),
            List.of("MN up", 4000),
            List.of(MAIN_SCENARIO_NAME, 4000)};
    if (withTotals) {
      Arrays.copyOf(lists, lists.length + 1);
      lists[lists.length - 1] = Arrays.asList(QueryEngine.GRAND_TOTAL, 5 * 4000);
      Assertions.assertThat(table.rows).containsExactlyInAnyOrder(lists);
    }
    Assertions.assertThat(table.rows).containsExactlyInAnyOrder(lists);
    Assertions.assertThat(table.columns).containsExactly(SCENARIO_FIELD_NAME, "sum(quantity)");
  }
}
