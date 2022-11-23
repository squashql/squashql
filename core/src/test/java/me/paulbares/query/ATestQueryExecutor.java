package me.paulbares.query;

import me.paulbares.query.builder.Query;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.OrderKeywordDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.List;

import static me.paulbares.query.Functions.*;
import static me.paulbares.query.database.QueryEngine.GRAND_TOTAL;
import static me.paulbares.query.database.QueryEngine.TOTAL;
import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static me.paulbares.transaction.TransactionManager.SCENARIO_FIELD_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestQueryExecutor {

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
    Field subcategory = new Field("subcategory", String.class);
    Field price = new Field("price", double.class);
    Field qty = new Field("quantity", int.class);

    this.datastore = createDatastore();
    QueryEngine queryEngine = createQueryEngine(this.datastore);
    this.queryExecutor = new QueryExecutor(queryEngine);
    this.tm = createTransactionManager();

    beforeLoad(List.of(ean, category, subcategory, price, qty));
    load();
  }

  protected void load() {
    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"bottle", "drink", null, 2d, 10},
            new Object[]{"cookie", "food", "biscuit", 3d, 20},
            new Object[]{"shirt", "cloth", null, 10d, 3}
    ));

    this.tm.load("s1", this.storeName, List.of(
            new Object[]{"bottle", "drink", null, 4d, 10},
            new Object[]{"cookie", "food", "biscuit", 3d, 20},
            new Object[]{"shirt", "cloth", null, 10d, 3}
    ));

    this.tm.load("s2", this.storeName, List.of(
            new Object[]{"bottle", "drink", null, 1.5d, 10},
            new Object[]{"cookie", "food", "biscuit", 3d, 20},
            new Object[]{"shirt", "cloth", null, 10d, 3}
    ));
  }

  protected void beforeLoad(List<Field> fields) {
  }

  @Test
  void testQueryWildcard() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(SCENARIO_FIELD_NAME), List.of(sum("p", "price"), sum("q", "quantity")))
            .build();
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of(MAIN_SCENARIO_NAME, 15.0d, 33l),
            List.of("s1", 17.0d, 33l),
            List.of("s2", 14.5d, 33l));
  }

  @Test
  void testQueryWildcardWithFullRollup() {
    QueryDto query = Query
            .from(this.storeName)
            .where(SCENARIO_FIELD_NAME, eq(MAIN_SCENARIO_NAME)) // use a filter to have a small output table
            .select(List.of(SCENARIO_FIELD_NAME, "category"), List.of(sum("p", "price"), sum("q", "quantity")))
            .rollup(SCENARIO_FIELD_NAME, "category")
            .build();
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList(GRAND_TOTAL, null, 15d, 33l),
            List.of(MAIN_SCENARIO_NAME, TOTAL, 15d, 33l),
            List.of(MAIN_SCENARIO_NAME, "cloth", 10d, 3l),
            List.of(MAIN_SCENARIO_NAME, "drink", 2d, 10l),
            List.of(MAIN_SCENARIO_NAME, "food", 3d, 20l));
  }

  /**
   * subcategory is null for some rows. The engine needs to be capable of distinguish null values that are returned by
   * ROLLUP from standard null values.
   */
  @Test
  void testQueryWildcardWithFullRollupWithNullValues() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("subcategory"), List.of(sum("q", "quantity")))
            .rollup("subcategory")
            .build();
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, 99l),
            List.of("biscuit", 60l),
            Arrays.asList(null, 39l));
  }

  @Test
  void testQueryWildcardPartialRollupWithTwoColumns() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(SCENARIO_FIELD_NAME, "category"), List.of(sum("q", "quantity")))
            .rollup("category") // We don't care here about total on scenario
            .build();
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of(MAIN_SCENARIO_NAME, TOTAL, 33l),
            List.of(MAIN_SCENARIO_NAME, "cloth", 3l),
            List.of(MAIN_SCENARIO_NAME, "drink", 10l),
            List.of(MAIN_SCENARIO_NAME, "food", 20l),
            List.of("s1", TOTAL, 33l),
            List.of("s1", "cloth", 3l),
            List.of("s1", "drink", 10l),
            List.of("s1", "food", 20l),
            List.of("s2", TOTAL, 33l),
            List.of("s2", "cloth", 3l),
            List.of("s2", "drink", 10l),
            List.of("s2", "food", 20l));

    query = Query
            .from(this.storeName)
            .select(List.of(SCENARIO_FIELD_NAME, "category"), List.of(sum("q", "quantity")))
            .rollup(SCENARIO_FIELD_NAME) // try with another column
            .build();
    result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of(TOTAL, "cloth", 9l),
            List.of(TOTAL, "drink", 30l),
            List.of(TOTAL, "food", 60l),
            List.of(MAIN_SCENARIO_NAME, "cloth", 3l),
            List.of(MAIN_SCENARIO_NAME, "drink", 10l),
            List.of(MAIN_SCENARIO_NAME, "food", 20l),
            List.of("s1", "cloth", 3l),
            List.of("s1", "drink", 10l),
            List.of("s1", "food", 20l),
            List.of("s2", "cloth", 3l),
            List.of("s2", "drink", 10l),
            List.of("s2", "food", 20l));
  }

  @Test
  void testQueryWildcardPartialRollupWithThreeColumns() {
    QueryDto query = Query
            .from(this.storeName)
            .where(SCENARIO_FIELD_NAME, eq("s1")) // filter to reduce output table size
            .select(List.of(SCENARIO_FIELD_NAME, "category", "subcategory"), List.of(sum("q", "quantity")))
            .rollup("category", "subcategory")
            .build();
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("s1", TOTAL, null, 33l),
            Arrays.asList("s1", "cloth", TOTAL, 3l),
            Arrays.asList("s1", "cloth", null, 3l),
            Arrays.asList("s1", "drink", TOTAL, 10l),
            Arrays.asList("s1", "drink", null, 10l),
            Arrays.asList("s1", "food", TOTAL, 20l),
            Arrays.asList("s1", "food", "biscuit", 20l));

    query = Query
            .from(this.storeName)
            .where(SCENARIO_FIELD_NAME, eq("s1")) // filter to reduce output table size
            .select(List.of(SCENARIO_FIELD_NAME, "category", "subcategory"), List.of(sum("q", "quantity")))
            .rollup("category") // Only total for category
            .build();
    result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("s1", TOTAL, "biscuit", 20l),
            Arrays.asList("s1", TOTAL, null, 13l),
            Arrays.asList("s1", "cloth", null, 3l),
            Arrays.asList("s1", "drink", null, 10l),
            Arrays.asList("s1", "food", "biscuit", 20l));
  }

  @Test
  void testQueryWildcardCount() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(SCENARIO_FIELD_NAME), List.of(CountMeasure.INSTANCE))
            .build();
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of(MAIN_SCENARIO_NAME, 3l),
            List.of("s1", 3l),
            List.of("s2", 3l));
    Assertions.assertThat(result.headers().stream().map(Field::name))
            .containsExactly(SCENARIO_FIELD_NAME, CountMeasure.ALIAS);
  }

  @Test
  void testQuerySeveralCoordinates() {
    QueryDto query = Query
            .from(this.storeName)
            .where(SCENARIO_FIELD_NAME, in("s1", "s2"))
            .select(List.of(SCENARIO_FIELD_NAME), List.of(sum("p", "price"), sum("q", "quantity")))
            .build();
    Table table = this.queryExecutor.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(
            List.of("s1", 17.0d, 33l),
            List.of("s2", 14.5d, 33l));
  }

  @Test
  void testQuerySingleCoordinate() {
    QueryDto query = Query
            .from(this.storeName)
            .where(SCENARIO_FIELD_NAME, eq("s1"))
            .select(List.of(SCENARIO_FIELD_NAME), List.of(sum("p", "price"), sum("q", "quantity")))
            .build();
    Table table = this.queryExecutor.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(List.of("s1", 17.0d, 33l));
  }

  @Test
  void testConditions() {
    QueryDto query = Query
            .from(this.storeName)
            .where(SCENARIO_FIELD_NAME, eq(MAIN_SCENARIO_NAME))
            .where("ean", eq("bottle"))
            .where("category", in("cloth", "drink"))
            .select(List.of("category", "ean"), List.of(sum("q", "quantity")))
            .build();

    Table table = this.queryExecutor.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(List.of("drink", "bottle", 10l));

    query.withCondition("quantity", Functions.gt(10));
    table = this.queryExecutor.execute(query);
    Assertions.assertThat(table).isEmpty();
  }

  @Test
  void testConditionsNullNotNull() {
    QueryDto query = Query.from(this.storeName)
            .where("subcategory", Functions.isNotNull())
            .select(List.of("ean"), List.of(CountMeasure.INSTANCE))
            .build();
    Table table = this.queryExecutor.execute(query);
    Assertions.assertThat(table).containsExactly(List.of("cookie", 3l));

    query = Query.from(this.storeName)
            .where("subcategory", Functions.isNull())
            .select(List.of("ean"), List.of(CountMeasure.INSTANCE))
            .build();
    table = this.queryExecutor.execute(query);
    Assertions.assertThat(table).containsExactly(
            List.of("bottle", 3l),
            List.of("shirt", 3l));
  }

  @Test
  void testLikeCondition() {
    QueryDto query = Query
            .from(this.storeName)
            .where(SCENARIO_FIELD_NAME, Functions.like("s%")) // start with s
            .select(List.of(SCENARIO_FIELD_NAME), List.of())
            .build();
    Table table = this.queryExecutor.execute(query);
    Assertions.assertThat(table).containsExactly(List.of("s1"), List.of("s2"));
  }

  /**
   * Without measure, we can use it to do a discovery.
   */
  @Test
  void testDiscovery() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(SCENARIO_FIELD_NAME), List.of())
            .build();
    Table table = this.queryExecutor.execute(query);
    Assertions.assertThat(table).containsExactly(
            List.of(MAIN_SCENARIO_NAME),
            List.of("s1"),
            List.of("s2"));
  }

  /**
   * https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators/#-if. Such function does not exist in
   * Spark.
   * <p>
   * {@code sumIf(quantity, category = 'food' OR category = 'drink')}
   */
  @Test
  void testSumIf() {
    ConditionDto or = eq("food").or(eq("drink"));
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(SCENARIO_FIELD_NAME),
                    List.of(new ExpressionMeasure("quantity if food or drink", "sum(case when category = 'food' OR category = 'drink' then quantity end)"),
                            Functions.sumIf("quantity filtered", "quantity", "category", or)))
            .build();
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
    QueryDto query = Query
            .from(this.storeName)
            .where("category", in("cloth", "drink"))
            .select(List.of(SCENARIO_FIELD_NAME, "category"), List.of(CountMeasure.INSTANCE))
            .build();
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of(MAIN_SCENARIO_NAME, "cloth", 1l),
            List.of(MAIN_SCENARIO_NAME, "drink", 1l),
            List.of("s1", "cloth", 1l),
            List.of("s1", "drink", 1l),
            List.of("s2", "cloth", 1l),
            List.of("s2", "drink", 1l));

    query.orderBy("category", OrderKeywordDto.DESC);
    result = this.queryExecutor.execute(query);
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
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("category"), List.of(sum("p", "price")))
            .build();
    Table result = this.queryExecutor.execute(query);
    // Default order
    Assertions.assertThat(result).containsExactly(
            List.of("cloth", 30d),
            List.of("drink", 7.5d),
            List.of("food", 9d));

    query.orderBy(result.getField(sum("p", "price")).name(), OrderKeywordDto.DESC);
    result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of("cloth", 30d),
            List.of("food", 9d),
            List.of("drink", 7.5d));
  }

  @Test
  void testConstantMeasures() {
    Measure integer = Functions.integer(100);
    Measure decimal = Functions.decimal(100);
    Measure ca = sum("ca", "price");
    Measure qty = sum("qty", "quantity");
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(), List.of(
                    multiply("a1", integer, ca),
                    multiply("a2", decimal, ca),
                    multiply("b1", integer, qty),
                    multiply("b2", decimal, qty),
                    integer,
                    decimal))
            .build();
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactly(List.of(4650d, 4650d, 9900l, 9900d, 100l, 100d));
    Assertions.assertThat(result.headers().stream().map(Field::name).toList())
            .containsExactly("a1", "a2", "b1", "b2", "constant(100)", "constant(100.0)");
  }
}
