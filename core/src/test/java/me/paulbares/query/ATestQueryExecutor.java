package me.paulbares.query;

import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.context.Repository;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.OrderDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static me.paulbares.transaction.TransactionManager.SCENARIO_FIELD_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestQueryExecutor {

  public static final String REPO_URL = "https://raw.githubusercontent.com/paulbares/aitm-assets/main/metrics-test.json";

  protected Datastore datastore;

  protected QueryExecutor queryExecutor;

  protected TransactionManager tm;

  protected String storeName = "myAwesomeStore";

  protected abstract QueryEngine createQueryEngine(Datastore datastore);

  protected abstract Datastore createDatastore();

  protected abstract TransactionManager createTransactionManager();

  @BeforeAll
  void setup() {
    Field ean = new Field("ean", String.class);
    Field category = new Field("category", String.class);
    Field price = new Field("price", double.class);
    Field qty = new Field("quantity", int.class);

    this.datastore = createDatastore();
    QueryEngine queryEngine = createQueryEngine(this.datastore);
    this.queryExecutor = new QueryExecutor(queryEngine);
    this.tm = createTransactionManager();

    beforeLoading(List.of(ean, category, price, qty));

    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"bottle", "drink", 2d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    this.tm.load("s1", this.storeName, List.of(
            new Object[]{"bottle", "drink", 4d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    this.tm.load("s2", this.storeName, List.of(
            new Object[]{"bottle", "drink", 1.5d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));
  }

  protected void beforeLoading(List<Field> fields) {
  }

  @Test
  void testQueryWildcard() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum");
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of(MAIN_SCENARIO_NAME, 15.0d, 33l),
            List.of("s1", 17.0d, 33l),
            List.of("s2", 14.5d, 33l));
  }

  @Test
  void testQueryWildcardCount() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .withMeasure(CountMeasure.INSTANCE);
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of(MAIN_SCENARIO_NAME, 3l),
            List.of("s1", 3l),
            List.of("s2", 3l));
    Assertions.assertThat(result.headers().stream().map(Field::name))
            .containsExactly(SCENARIO_FIELD_NAME, CountMeasure.ALIAS);
  }

  @Test
  void testQuerySeveralCoordinates() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .withCondition(SCENARIO_FIELD_NAME, QueryBuilder.in("s1", "s2"))
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum");
    Table table = this.queryExecutor.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(
            List.of("s1", 17.0d, 33l),
            List.of("s2", 14.5d, 33l));
  }

  @Test
  void testQuerySingleCoordinate() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .withCondition(SCENARIO_FIELD_NAME, QueryBuilder.eq("s1"))
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum");
    Table table = this.queryExecutor.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(List.of("s1", 17.0d, 33l));
  }

  @Test
  void testConditions() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn("category")
            .withColumn("ean")
            .aggregatedMeasure("quantity", "sum")
            .withCondition(SCENARIO_FIELD_NAME, QueryBuilder.eq(MAIN_SCENARIO_NAME))
            .withCondition("ean", QueryBuilder.eq("bottle"))
            .withCondition("category", QueryBuilder.in("cloth", "drink"));

    Table table = this.queryExecutor.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(List.of("drink", "bottle", 10l));

    query.withCondition("quantity", QueryBuilder.gt(10));
    table = this.queryExecutor.execute(query);
    Assertions.assertThat(table).isEmpty();
  }

  /**
   * Without measure, we can use it to do a discovery.
   */
  @Test
  void testDiscovery() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME);
    Table table = this.queryExecutor.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(
            List.of(MAIN_SCENARIO_NAME),
            List.of("s1"),
            List.of("s2"));
  }

  @Test
  void testJsonConverter() throws Exception {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum");
    Table table = this.queryExecutor.execute(query);
    String actual = JacksonUtil.tableToCsv(table);
    Map map = JacksonUtil.mapper.readValue(actual, Map.class);
    Assertions.assertThat((List) map.get("columns")).containsExactly(SCENARIO_FIELD_NAME, "sum(price)", "sum(quantity)");
    Assertions.assertThat((List) map.get("rows")).containsExactlyInAnyOrder(
            List.of(MAIN_SCENARIO_NAME, 15d, 33),
            List.of("s1", 17d, 33),
            List.of("s2", 14.5d, 33));
  }

  @Test
  void testQueryWithRepository() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .unresolvedExpressionMeasure("price")
            .unresolvedExpressionMeasure("quantity")
            .context(Repository.KEY, new Repository(REPO_URL));

    Table table = this.queryExecutor.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(
            List.of(MAIN_SCENARIO_NAME, 15.0d, 33l),
            List.of("s1", 17.0d, 33l),
            List.of("s2", 14.5d, 33l));
  }

  @Test
  void testQueryWithRepositoryMeasureDoesNotExist() {
    String notexistingmeasure = "notexistingmeasure";
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .unresolvedExpressionMeasure("price")
            .unresolvedExpressionMeasure(notexistingmeasure)
            .context(Repository.KEY, new Repository(REPO_URL));

    Assertions.assertThatThrownBy(() -> this.queryExecutor.execute(query))
            .hasMessageContaining("Cannot find expression with alias " + notexistingmeasure);
  }

  /**
   * https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators/#-if. Such function does not exist in
   * Spark.
   * <p>
   * {@code sumIf(quantity, category = 'food' OR category = 'drink')}
   */
  @Test
  void testSumIf() {
    ConditionDto or = QueryBuilder.eq("food").or(QueryBuilder.eq("drink"));

    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .expressionMeasure(
                    "quantity if food or drink",
                    "sum(case when category = 'food' OR category = 'drink' then quantity end)")
            .aggregatedMeasure("quantity filtered", "quantity", "sum", "category", or);
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of(MAIN_SCENARIO_NAME, 30l, 30l),
            List.of("s1", 30l, 30l),
            List.of("s2", 30l, 30l));
    Assertions.assertThat(result.headers().stream().map(Field::name))
            .containsExactly(SCENARIO_FIELD_NAME, "quantity if food or drink", "quantity filtered");
  }

  @Test
  void testOrderByColumn() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .withColumn("category")
            .withCondition("category", QueryBuilder.in("cloth", "drink"))
            .withMeasure(CountMeasure.INSTANCE);
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of(MAIN_SCENARIO_NAME, "cloth", 1l),
            List.of(MAIN_SCENARIO_NAME, "drink", 1l),
            List.of("s1", "cloth", 1l),
            List.of("s1", "drink", 1l),
            List.of("s2", "cloth", 1l),
            List.of("s2", "drink", 1l));

    query.orderBy("category", OrderDto.DESC);
    result = this.queryExecutor.execute(query);
    result.show();
    Assertions.assertThat(result).containsExactly(
            List.of(MAIN_SCENARIO_NAME, "drink", 1l),
            List.of(MAIN_SCENARIO_NAME, "cloth", 1l),
            List.of("s1", "drink", 1l),
            List.of("s1", "cloth", 1l),
            List.of("s2", "drink", 1l),
            List.of("s2", "cloth", 1l));

    List<String> elements = List.of("s2", MAIN_SCENARIO_NAME, "s1");
    query.orderBy(SCENARIO_FIELD_NAME, elements);
    result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of("s2", "drink", 1l),
            List.of("s2", "cloth", 1l),
            List.of(MAIN_SCENARIO_NAME, "drink", 1l),
            List.of(MAIN_SCENARIO_NAME, "cloth", 1l),
            List.of("s1", "drink", 1l),
            List.of("s1", "cloth", 1l));
  }

  @Test
  void testOrderByMeasure() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn("category")
            .withMeasure(new AggregatedMeasure("price", AggregationFunction.SUM));
    Table result = this.queryExecutor.execute(query);
    // Default order
    Assertions.assertThat(result).containsExactly(
            List.of("cloth", 30d),
            List.of("drink", 7.5d),
            List.of("food", 9d));

    query.orderBy(result.getField(new AggregatedMeasure("price", AggregationFunction.SUM)).name(), OrderDto.DESC);
    result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of("cloth", 30d),
            List.of("food", 9d),
            List.of("drink", 7.5d));
  }
}
