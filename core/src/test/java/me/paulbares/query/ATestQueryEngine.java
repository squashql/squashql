package me.paulbares.query;

import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.context.Repository;
import me.paulbares.query.dto.ConditionType;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.SingleValueConditionDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static me.paulbares.query.QueryBuilder.TOP;
import static me.paulbares.query.QueryEngine.GRAND_TOTAL;
import static me.paulbares.query.QueryEngine.TOTAL;
import static me.paulbares.query.context.Totals.KEY;
import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;
import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestQueryEngine {

  private static final String REPO_URL = "https://raw.githubusercontent.com/paulbares/aitm-assets/main/metrics-test.json";

  protected Datastore datastore;

  protected QueryEngine queryEngine;

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
    this.queryEngine = createQueryEngine(this.datastore);
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
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum");
    Table result = this.queryEngine.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("base", 15.0d, 33l),
            List.of("s1", 17.0d, 33l),
            List.of("s2", 14.5d, 33l));
  }

  @Test
  void testQueryWildcardCount() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("*", "count");
    Table result = this.queryEngine.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("base", 3l),
            List.of("s1", 3l),
            List.of("s2", 3l));
  }

  @Test
  void testQueryWildcardWithTotals() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum")
            .context(KEY, TOP);
    Table table = this.queryEngine.execute(query);
    Assertions.assertThat(table).containsExactly(
            List.of(GRAND_TOTAL, 15.d + 17.d + 14.5, 33 * 3l),
            List.of("base", 15.0d, 33l),
            List.of("s1", 17.0d, 33l),
            List.of("s2", 14.5d, 33l));
  }

  @Test
  void testQueryWildcardAndCrossjoinWithTotals() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .wildcardCoordinate("category")
            .wildcardCoordinate("ean")
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum")
            .context(KEY, TOP);

    Table table = this.queryEngine.execute(query);
    Assertions.assertThat(table).containsExactly(
            Arrays.asList(GRAND_TOTAL, null, null, 15.d + 17.d + 14.5d, 33 * 3l),
            Arrays.asList("base", TOTAL, null,  15.0d, 33l),
            Arrays.asList("base", "cloth", TOTAL, 10.0d, 3l),
            Arrays.asList("base", "cloth", "shirt", 10.0d, 3l),
            Arrays.asList("base", "drink", TOTAL, 2.0d, 10l),
            Arrays.asList("base", "drink", "bottle", 2.0d, 10l),
            Arrays.asList("base", "food", TOTAL, 3.0d, 20l),
            Arrays.asList("base", "food", "cookie", 3.0d, 20l),

            Arrays.asList("s1", TOTAL, null,  17.0d, 33l),
            Arrays.asList("s1", "cloth", TOTAL, 10.0d, 3l),
            Arrays.asList("s1", "cloth", "shirt", 10.0d, 3l),
            Arrays.asList("s1", "drink", TOTAL, 4.0d, 10l),
            Arrays.asList("s1", "drink", "bottle", 4.0d, 10l),
            Arrays.asList("s1", "food", TOTAL, 3.0d, 20l),
            Arrays.asList("s1", "food", "cookie", 3.0d, 20l),

            Arrays.asList("s2", TOTAL, null,  14.5d, 33l),
            Arrays.asList("s2", "cloth", TOTAL, 10.0d, 3l),
            Arrays.asList("s2", "cloth", "shirt", 10.0d, 3l),
            Arrays.asList("s2", "drink", TOTAL, 1.5d, 10l),
            Arrays.asList("s2", "drink", "bottle", 1.5d, 10l),
            Arrays.asList("s2", "food", TOTAL, 3.0d, 20l),
            Arrays.asList("s2", "food", "cookie", 3.0d, 20l));
  }

  @Test
  void testQueryWildcardAndCrossjoinWithTotalsPositionBottom() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .wildcardCoordinate("category")
            .wildcardCoordinate("ean")
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum")
            .context(KEY, QueryBuilder.BOTTOM);
    Table table = this.queryEngine.execute(query);
    Assertions.assertThat(table).containsExactly(
            Arrays.asList("base", "cloth", "shirt", 10.0d, 3l),
            Arrays.asList("base", "cloth", TOTAL, 10.0d, 3l),
            Arrays.asList("base", "drink", "bottle", 2.0d, 10l),
            Arrays.asList("base", "drink", TOTAL, 2.0d, 10l),
            Arrays.asList("base", "food", "cookie", 3.0d, 20l),
            Arrays.asList("base", "food", TOTAL, 3.0d, 20l),
            Arrays.asList("base", TOTAL, null,  15.0d, 33l),

            Arrays.asList("s1", "cloth", "shirt", 10.0d, 3l),
            Arrays.asList("s1", "cloth", TOTAL, 10.0d, 3l),
            Arrays.asList("s1", "drink", "bottle", 4.0d, 10l),
            Arrays.asList("s1", "drink", TOTAL, 4.0d, 10l),
            Arrays.asList("s1", "food", "cookie", 3.0d, 20l),
            Arrays.asList("s1", "food", TOTAL, 3.0d, 20l),
            Arrays.asList("s1", TOTAL, null,  17.0d, 33l),

            Arrays.asList("s2", "cloth", "shirt", 10.0d, 3l),
            Arrays.asList("s2", "cloth", TOTAL, 10.0d, 3l),
            Arrays.asList("s2", "drink", "bottle", 1.5d, 10l),
            Arrays.asList("s2", "drink", TOTAL, 1.5d, 10l),
            Arrays.asList("s2", "food", "cookie", 3.0d, 20l),
            Arrays.asList("s2", "food", TOTAL, 3.0d, 20l),
            Arrays.asList("s2", TOTAL, null,  14.5d, 33l),
            Arrays.asList(GRAND_TOTAL, null, null, 15.d + 17.d + 14.5d, 33 * 3l));
  }

  @Test
  void testQuerySeveralCoordinates() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .coordinates(SCENARIO_FIELD_NAME, "s1", "s2")
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum");
    Table table = this.queryEngine.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(
            List.of("s1", 17.0d, 33l),
            List.of("s2", 14.5d, 33l));
  }

  @Test
  void testQuerySingleCoordinate() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .coordinate(SCENARIO_FIELD_NAME, "s1")
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum");
    Table table = this.queryEngine.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(List.of("s1", 17.0d, 33l));
  }

  @Test
  void testConditions() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate("category")
            .wildcardCoordinate("ean")
            .aggregatedMeasure("quantity", "sum")
            .condition(SCENARIO_FIELD_NAME, new SingleValueConditionDto(ConditionType.EQ, MAIN_SCENARIO_NAME))
            .condition("ean", new SingleValueConditionDto(ConditionType.EQ, "bottle"))
            .condition("category", new SingleValueConditionDto(ConditionType.IN, Set.of("cloth", "drink")));
    Table table = this.queryEngine.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(List.of("drink", "bottle", 10l));

    query.condition("quantity", QueryBuilder.gt(10));
    table = this.queryEngine.execute(query);
    Assertions.assertThat(table).isEmpty();
  }

  @Test
  void testConditionsOnScenario() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("quantity", "sum")
            .condition(SCENARIO_FIELD_NAME, new SingleValueConditionDto(ConditionType.IN, Set.of("s1", "s2")))
            .context(KEY, TOP);
    Table table = this.queryEngine.execute(query);
    Assertions.assertThat(table).containsExactly(
            Arrays.asList(GRAND_TOTAL, 33 * 2l),
            Arrays.asList("s1", 33l),
            Arrays.asList("s2", 33l));
  }

  /**
   * Without measure, we can use it to do a discovery.
   */
  @Test
  void testDiscovery() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate(SCENARIO_FIELD_NAME);
    Table table = this.queryEngine.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(
                    List.of(MAIN_SCENARIO_NAME),
                    List.of("s1"),
                    List.of("s2"));
  }

  @Test
  void testJsonConverter() throws Exception {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum");
    Table table = this.queryEngine.execute(query);
    String actual = JacksonUtil.tableToCsv(table);
    Map map = JacksonUtil.mapper.readValue(actual, Map.class);
    Assertions.assertThat((List) map.get("columns")).containsExactly(SCENARIO_FIELD_NAME, "sum(price)", "sum(quantity)");
    Assertions.assertThat((List) map.get("rows")).containsExactlyInAnyOrder(
                    List.of("base", 15d, 33),
                    List.of("s1", 17d, 33),
                    List.of("s2", 14.5d, 33));
  }

  @Test
  void testQueryWithRepository() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .unresolvedExpressionMeasure("price")
            .unresolvedExpressionMeasure("quantity")
            .context(Repository.KEY, new Repository(REPO_URL));

    Table table = this.queryEngine.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(
            List.of("base", 15.0d, 33l),
            List.of("s1", 17.0d, 33l),
            List.of("s2", 14.5d, 33l));
  }

  @Test
  void testQueryWithRepositoryMeasureDoesNotExist() {
    String notexistingmeasure = "notexistingmeasure";
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .unresolvedExpressionMeasure("price")
            .unresolvedExpressionMeasure(notexistingmeasure)
            .context(Repository.KEY, new Repository(REPO_URL));

    Assertions.assertThatThrownBy(() -> this.queryEngine.execute(query))
            .hasMessageContaining("Cannot find expression with alias " + notexistingmeasure);
  }
}
