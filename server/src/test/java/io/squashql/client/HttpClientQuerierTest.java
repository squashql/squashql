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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.squashql.query.Functions.criterion;
import static io.squashql.query.Functions.eq;
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.transaction.DataLoader.MAIN_SCENARIO_NAME;
import static io.squashql.transaction.DataLoader.SCENARIO_FIELD_NAME;

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

  HttpClientQuerier querier;

  @LocalServerPort
  int port;

  @BeforeEach
  void before() {
    this.querier = new HttpClientQuerier("http://127.0.0.1:" + this.port);
  }

  @Test
  void testGetMetadata() {
    QueryControllerTest.assertMetadataResult(this.querier.metadata());
  }

  @Test
  void testRunQuery() {
    QueryDto query = new QueryDto()
            .table("our_prices")
            .withColumn(tableField(SCENARIO_FIELD_NAME))
            .withMeasure(new AggregatedMeasure("qs", "quantity", "sum"));

    QueryResultDto response = this.querier.run(query);
    assertQuery(response.table);
    Assertions.assertThat(response.metadata).containsExactly(
            new MetadataItem(SCENARIO_FIELD_NAME, SCENARIO_FIELD_NAME, String.class),
            new MetadataItem("qs", "qs", long.class));
//            new MetadataItem("qs", "sum(quantity)", long.class));

    Assertions.assertThat(response.debug.cache).isNotNull();
  }

  @Test
  void testMergeQuery() {
    QueryDto query1 = new QueryDto()
            .table("our_prices")
            .withColumn(tableField(SCENARIO_FIELD_NAME))
            .withMeasure(new AggregatedMeasure("qs", "quantity", "sum"));
    QueryDto query2 = new QueryDto()
            .table("our_prices")
            .withColumn(tableField(SCENARIO_FIELD_NAME))
            .withMeasure(new AggregatedMeasure(("qa"), "quantity", "avg"));

    QueryResultDto response = this.querier.queryMerge(new QueryMergeDto(query1, query2, JoinType.FULL));
    Assertions.assertThat(response.table.rows).containsExactlyInAnyOrder(List.of("MDD up", 4000, 1000d),
            List.of("MN & MDD down", 4000, 1000d),
            List.of("MN & MDD up", 4000, 1000d),
            List.of("MN up", 4000, 1000d),
            List.of(MAIN_SCENARIO_NAME, 4000, 1000d));
    Assertions.assertThat(response.table.columns).containsExactly(SCENARIO_FIELD_NAME, "qs", "qa");
    Assertions.assertThat(response.metadata).containsExactly(
            new MetadataItem(SCENARIO_FIELD_NAME, SCENARIO_FIELD_NAME, String.class),
            new MetadataItem("qs", "qs", long.class),
            new MetadataItem("qa", "qa", double.class));
//            new MetadataItem("qs", "sum(quantity)", long.class),
//            new MetadataItem("qa", "avg(quantity)", double.class));

    Assertions.assertThat(response.debug).isNull();
  }

  @Test
  void testRunGroupingScenarioQuery() {
    BucketColumnSetDto bucketCS = new BucketColumnSetDto("group", tableField(SCENARIO_FIELD_NAME))
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "MN up"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "MN & MDD up"))
            .withNewBucket("group3", List.of(MAIN_SCENARIO_NAME, "MN up", "MN & MDD up"));

    Measure aggregatedMeasure = Functions.sum("capdv", "capdv");
    ComparisonMeasureReferencePosition capdvDiff = new ComparisonMeasureReferencePosition(
            "capdvDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            aggregatedMeasure,
            Map.of(
                    tableField(SCENARIO_FIELD_NAME), "first",
                    tableField("group"), "g"
            ),
            ColumnSetKey.BUCKET);
    var query = Query
            .from("our_prices")
            .select_(List.of(bucketCS), List.of(capdvDiff, aggregatedMeasure))
            .build();

    QueryResultDto response = this.querier.run(query);
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
            .where(tableField(SCENARIO_FIELD_NAME), Functions.eq(MAIN_SCENARIO_NAME))
            .select(tableFields(List.of(SCENARIO_FIELD_NAME, "pdv")), List.of(Functions.sum("ps", "price")))
            .build();

    QueryResultDto response = this.querier.run(query);
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

    List<Measure> expression = this.querier.expression(input);
    Assertions.assertThat(expression.stream().map(Measure::expression)).containsExactly("sum(a)", "sum(b)", "a + b");
  }

  @Test
  void testPivotTable() {
    QueryDto query = Query.from("our_prices")
            .select(tableFields(List.of("ean", "pdv")), List.of(CountMeasure.INSTANCE))
            .build();
    PivotTableQueryDto pivotTableQuery = new PivotTableQueryDto(query, tableFields(List.of("pdv")), tableFields(List.of("ean")));
    PivotTableQueryResultDto response = this.querier.run(pivotTableQuery);

    Assertions.assertThat(response.rows).containsExactlyElementsOf(pivotTableQuery.rows.stream().map(NamedField::name).toList());
    Assertions.assertThat(response.columns).containsExactlyElementsOf(pivotTableQuery.columns.stream().map(NamedField::name).toList());
    Assertions.assertThat(response.values).containsExactlyElementsOf(List.of(CountMeasure.INSTANCE.alias));
    Assertions.assertThat(response.queryResult.table.rows)
            .containsExactly(
                    List.of(GRAND_TOTAL, GRAND_TOTAL, 20),
                    List.of(GRAND_TOTAL, "ITM Balma", 10),
                    List.of(GRAND_TOTAL, "ITM Toulouse and Drive", 10),
                    List.of("ITMella 250g", GRAND_TOTAL, 10),
                    List.of("ITMella 250g", "ITM Balma", 5),
                    List.of("ITMella 250g", "ITM Toulouse and Drive", 5),

                    List.of("Nutella 250g", GRAND_TOTAL, 10),
                    List.of("Nutella 250g", "ITM Balma", 5),
                    List.of("Nutella 250g", "ITM Toulouse and Drive", 5));
  }

  @Test
  void testRunQueryWithTotalCount() {
    // Note. The CJ will make null appear in rows. We want to make sure null values are correctly handled.
    QueryDto query = Query
            .from("our_prices")
            .where(criterion("pdv", eq("ITM Balma")))
            .select(tableFields(List.of(SCENARIO_FIELD_NAME, "pdv")), List.of(CountMeasure.INSTANCE, TotalCountMeasure.INSTANCE))
            .limit(2)
            .build();

    QueryResultDto response = this.querier.run(query);
    SimpleTableDto table = response.table;
    final int totalCountIdx = table.columns.indexOf(TotalCountMeasure.ALIAS);
    Assertions.assertThat(table.rows.stream().mapToInt(row -> (int) row.get(totalCountIdx))).containsOnly(5);
  }
}
