package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.*;
import io.squashql.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.squashql.query.Functions.*;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;
import static io.squashql.query.dto.OrderKeywordDto.ASC;
import static io.squashql.query.dto.OrderKeywordDto.DESC;
import static io.squashql.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static io.squashql.transaction.TransactionManager.SCENARIO_FIELD_NAME;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestQueryExecutor extends ABaseTestQuery {

  protected String storeName = "store" + getClass().getSimpleName().toLowerCase();

  @Override
  protected Map<String, List<Field>> getFieldsByStore() {
    Field ean = new Field(this.storeName, "ean", String.class);
    Field eanId = new Field(this.storeName, "eanId", int.class);
    Field category = new Field(this.storeName, "category", String.class);
    Field subcategory = new Field(this.storeName, "subcategory", String.class);
    Field price = new Field(this.storeName, "price", double.class);
    Field qty = new Field(this.storeName, "quantity", int.class);
    Field isFood = new Field(this.storeName, "isFood", boolean.class);
    return Map.of(this.storeName, List.of(eanId, ean, category, subcategory, price, qty, isFood));
  }

  @Override
  protected void loadData() {
    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{0, "bottle", "drink", null, 2d, 10, true},
            new Object[]{1, "cookie", "food", "biscuit", 3d, 20, true},
            new Object[]{2, "shirt", "cloth", null, 10d, 3, false}
    ));

    this.tm.load("s1", this.storeName, List.of(
            new Object[]{0, "bottle", "drink", null, 4d, 10, true},
            new Object[]{1, "cookie", "food", "biscuit", 3d, 20, true},
            new Object[]{2, "shirt", "cloth", null, 10d, 3, false}
    ));

    this.tm.load("s2", this.storeName, List.of(
            new Object[]{0, "bottle", "drink", null, 1.5d, 10, true},
            new Object[]{1, "cookie", "food", "biscuit", 3d, 20, true},
            new Object[]{2, "shirt", "cloth", null, 10d, 3, false}
    ));
  }

  @Test
  void testQueryWildcard() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(SCENARIO_FIELD_NAME), List.of(sum("p", "price"), sum("q", "quantity")))
            .build();
    Table result = this.executor.execute(query);
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
    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, GRAND_TOTAL, 15d, 33l),
            List.of(MAIN_SCENARIO_NAME, TOTAL, 15d, 33l),
            List.of(MAIN_SCENARIO_NAME, "cloth", 10d, 3l),
            List.of(MAIN_SCENARIO_NAME, "drink", 2d, 10l),
            List.of(MAIN_SCENARIO_NAME, "food", 3d, 20l));
  }

  /**
   * Same as {@link #testQueryWildcardWithFullRollup()} but name of columns are explicit i.e. full name.
   */
  @Test
  void testQueryWildcardWithFullRollupFullName() {
    QueryDto query = Query
            .from(this.storeName)
            .where(SqlUtils.getFieldFullName(this.storeName, SCENARIO_FIELD_NAME), eq(MAIN_SCENARIO_NAME)) // use a filter to have a small output table
            .select(List.of(SqlUtils.getFieldFullName(this.storeName, SCENARIO_FIELD_NAME), SqlUtils.getFieldFullName(this.storeName, "category")), List.of(sum("p", "price"), sum("q", "quantity")))
            .rollup(SqlUtils.getFieldFullName(this.storeName, SCENARIO_FIELD_NAME), SqlUtils.getFieldFullName(this.storeName, "category"))
            .build();
    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, GRAND_TOTAL, 15d, 33l),
            List.of(MAIN_SCENARIO_NAME, TOTAL, 15d, 33l),
            List.of(MAIN_SCENARIO_NAME, "cloth", 10d, 3l),
            List.of(MAIN_SCENARIO_NAME, "drink", 2d, 10l),
            List.of(MAIN_SCENARIO_NAME, "food", 3d, 20l));
  }

  @Test
  void testQueryWildcardWithFullRollupOnColumnTypeInt() {
    QueryDto query = Query
            .from(this.storeName)
            .where(SCENARIO_FIELD_NAME, eq(MAIN_SCENARIO_NAME)) // use a filter to have a small output table
            .select(List.of("eanId"), List.of(sum("p", "price"), sum("q", "quantity")))
            .rollup("eanId")
            .build();
    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, 15d, 33l),
            List.of(translate(0), 2d, 10l),
            List.of(translate(1), 3d, 20l),
            List.of(translate(2), 10d, 3l));
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
    Table result = this.executor.execute(query);
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
    Table result = this.executor.execute(query);
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
    result = this.executor.execute(query);
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
    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("s1", TOTAL, TOTAL, 33l),
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
    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("s1", TOTAL, "biscuit", 20l),
            Arrays.asList("s1", TOTAL, null, 13l),
            Arrays.asList("s1", "cloth", null, 3l),
            Arrays.asList("s1", "drink", null, 10l),
            Arrays.asList("s1", "food", "biscuit", 20l));
  }

  @Test
  void testQueryRollupIncorrect() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("category"), List.of(sum("p", "price")))
            .rollup("subcategory") // not correct because it should be a subset of the column in the select
            .build();
    Assertions.assertThatThrownBy(() -> this.executor.execute(query))
            // The columns contain in rollup [`subcategory`] must be a subset of the columns contain in the select [`category`]
            .hasMessageContaining("must be a subset of the columns contain in the select");
  }

  @Test
  void testQueryWildcardCount() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(SCENARIO_FIELD_NAME), List.of(CountMeasure.INSTANCE))
            .build();
    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of(MAIN_SCENARIO_NAME, 3l),
            List.of("s1", 3l),
            List.of("s2", 3l));
    Assertions.assertThat(result.headers().stream().map(Header::field).map(Field::name))
            .containsExactly(SCENARIO_FIELD_NAME, CountMeasure.ALIAS);
  }

  @Test
  void testQuerySeveralCoordinates() {
    QueryDto query = Query
            .from(this.storeName)
            .where(SCENARIO_FIELD_NAME, in("s1", "s2"))
            .select(List.of(SCENARIO_FIELD_NAME), List.of(sum("p", "price"), sum("q", "quantity")))
            .build();
    Table table = this.executor.execute(query);
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
    Table table = this.executor.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(List.of("s1", 17.0d, 33l));
  }

  @Test
  void testConditions() {
    QueryDto query = Query
            .from(this.storeName)
            .where(Functions.all(
                    criterion("ean", eq("bottle")),
                    criterion(SCENARIO_FIELD_NAME, eq(MAIN_SCENARIO_NAME)),
                    criterion("category", in("cloth", "drink"))))
            .select(List.of("category", "ean"), List.of(sum("q", "quantity")))
            .build();

    Table table = this.executor.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(List.of("drink", "bottle", 10l));

    query = Query
            .from(this.storeName)
            .where(Functions.all(
                    criterion("ean", eq("bottle")),
                    criterion(SCENARIO_FIELD_NAME, eq(MAIN_SCENARIO_NAME)),
                    criterion("category", in("cloth", "drink")),
                    criterion("quantity", Functions.gt(10))))
            .select(List.of("category", "ean"), List.of(sum("q", "quantity")))
            .build();
    table = this.executor.execute(query);
    Assertions.assertThat(table).isEmpty();
  }

  @Test
  void testConditionsNullNotNull() {
    QueryDto query = Query.from(this.storeName)
            .where(Functions.criterion("subcategory", Functions.isNotNull()))
            .select(List.of("ean"), List.of(CountMeasure.INSTANCE))
            .build();
    Table table = this.executor.execute(query);
    Assertions.assertThat(table).containsExactly(List.of("cookie", 3l));

    query = Query.from(this.storeName)
            .where(Functions.criterion("subcategory", Functions.isNull()))
            .select(List.of("ean"), List.of(CountMeasure.INSTANCE))
            .build();
    table = this.executor.execute(query);
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
    Table table = this.executor.execute(query);
    Assertions.assertThat(table).containsExactly(List.of("s1"), List.of("s2"));
  }

  @Test
  void testBooleanCondition() {
    QueryDto query = Query
            .from(this.storeName)
            .where(criterion("isFood", eq(true)))
            .select(List.of("ean"), List.of())
            .build();
    Table table = this.executor.execute(query);
    Assertions.assertThat(table).containsExactly(List.of("bottle"), List.of("cookie"));
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
    Table table = this.executor.execute(query);
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
    Assumptions.assumeFalse(this.queryEngine.getClass().getSimpleName().equals("SnowflakeQueryEngine"));

    ConditionDto or = eq("food").or(eq("drink"));
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(SCENARIO_FIELD_NAME),
                    List.of(new ExpressionMeasure("quantity if food or drink", "sum(case when category = 'food' OR category = 'drink' then quantity end)"),
                            sumIf("quantity filtered", "quantity", criterion("category", or))))
            .build();
    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of(MAIN_SCENARIO_NAME, 30l, 30l),
            List.of("s1", 30l, 30l),
            List.of("s2", 30l, 30l));
    Assertions.assertThat(result.headers().stream().map(Header::field).map(Field::name))
            .containsExactly(SCENARIO_FIELD_NAME, "quantity if food or drink", "quantity filtered");

    // Mutliple fields
    CriteriaDto category = criterion("category", or);
    query = Query
            .from(this.storeName)
            .select(List.of(SCENARIO_FIELD_NAME),
                    List.of(sumIf("quantity filtered", "quantity", Functions.all(category, criterion("subcategory", eq("biscuit"))))))
            .build();
    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of(MAIN_SCENARIO_NAME, 20l),
            List.of("s1", 20l),
            List.of("s2", 20l));
  }

  @Test
  void testOrderByColumn() {
    QueryDto query = Query
            .from(this.storeName)
            .where("category", in("cloth", "drink"))
            .select(List.of(SCENARIO_FIELD_NAME, "category"), List.of(CountMeasure.INSTANCE))
            .build();
    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of(MAIN_SCENARIO_NAME, "cloth", 1l),
            List.of(MAIN_SCENARIO_NAME, "drink", 1l),
            List.of("s1", "cloth", 1l),
            List.of("s1", "drink", 1l),
            List.of("s2", "cloth", 1l),
            List.of("s2", "drink", 1l));

    query.orderBy("category", OrderKeywordDto.DESC);
    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of(MAIN_SCENARIO_NAME, "drink", 1l),
            List.of(MAIN_SCENARIO_NAME, "cloth", 1l),
            List.of("s1", "drink", 1l),
            List.of("s1", "cloth", 1l),
            List.of("s2", "drink", 1l),
            List.of("s2", "cloth", 1l));

    List<String> elements = List.of("s2", MAIN_SCENARIO_NAME, "s1");
    query.orderBy(SCENARIO_FIELD_NAME, elements);
    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of("s2", "drink", 1l),
            List.of("s2", "cloth", 1l),
            List.of(MAIN_SCENARIO_NAME, "drink", 1l),
            List.of(MAIN_SCENARIO_NAME, "cloth", 1l),
            List.of("s1", "drink", 1l),
            List.of("s1", "cloth", 1l));
  }

  @Test
  void testOrderByWithRollup() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("category"), List.of(sum("p_sum", "price")))
            .rollup(List.of("category"))
            .orderBy("category", ASC)
            .build();
    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, 46.5d),
            List.of("cloth", 30d),
            List.of("drink", 7.5d),
            List.of("food", 9d));

    query.orderBy("category", DESC);
    result = this.executor.execute(query);
    // Total AND Grand Total always on top
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, 46.5d),
            List.of("food", 9d),
            List.of("drink", 7.5d),
            List.of("cloth", 30d));

    // With explicit ordering
    query.orderBy("category", List.of("drink", "food", "cloth"));
    result = this.executor.execute(query);
    // Total AND Grand Total always on top
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, 46.5d),
            List.of("drink", 7.5d),
            List.of("food", 9d),
            List.of("cloth", 30d));
  }

  @Test
  void testOrderByColumnWithNullValues() {
    // Without explicit ordering
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("subcategory"), List.of(CountMeasure.INSTANCE))
            .build();
    Table result = this.executor.execute(query);
    // Without explicit ordering, null comes last because of: Comparator.nullsLast(Comparator.naturalOrder())
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("biscuit", 3l),
            Arrays.asList(null, 6l));

    // With explicit ordering
    query = Query
            .from(this.storeName)
            .select(List.of("subcategory"), List.of(CountMeasure.INSTANCE))
            .orderBy("subcategory", ASC)
            .build();
    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("biscuit", 3l),
            Arrays.asList(null, 6l));
  }

  @Test
  void testOrderByMeasure() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("category"), List.of(sum("p", "price")))
            .build();
    Table result = this.executor.execute(query);
    // Default order
    Assertions.assertThat(result).containsExactly(
            List.of("cloth", 30d),
            List.of("drink", 7.5d),
            List.of("food", 9d));

    query.orderBy(result.getField(sum("p", "price")).name(), OrderKeywordDto.DESC);
    result = this.executor.execute(query);
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
    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(List.of(4650d, 4650d, 9900l, 9900d, 100l, 100d));
    Assertions.assertThat(result.headers().stream().map(Header::field).map(Field::name).toList())
            .containsExactly("a1", "a2", "b1", "b2", "constant(100)", "constant(100.0)");
  }

  @Test
  void testRawQueryExecution() {
    String tableName = this.executor.queryEngine.queryRewriter().tableName(this.storeName);
    Table result = this.executor.execute(String.format("select ean, sum(price) as sumprice from %s group by ean order by ean", tableName));
    Assertions.assertThat(result.headers().stream().map(header -> header.field().name()).toList())
            .containsExactly("ean", "sumprice");
    Assertions.assertThat(result).containsExactly(
            List.of("bottle", 7.5d),
            List.of("cookie", 9.0d),
            List.of("shirt", 30d));
  }

  @Test
  void testMergeTables() {
    QueryDto query1 = Query
            .from(this.storeName)
            .select(List.of("category"), List.of(sum("p_sum", "price")))
            .rollup(List.of("category"))
            .build();

    QueryDto query2 = Query
            .from(this.storeName)
            .select(List.of("category", SCENARIO_FIELD_NAME), List.of(min("p_min", "price")))
            .rollup(List.of("category", SCENARIO_FIELD_NAME))
            .build();

    Table result = this.executor.execute(query1, query2, null);

    Assertions.assertThat(result.headers().stream().map(Header::field).map(Field::name).toList())
            .containsExactly("category", SCENARIO_FIELD_NAME, "p_sum", "p_min");
    Assertions.assertThat(result).containsExactly(
            Arrays.asList(GRAND_TOTAL, GRAND_TOTAL, 46.5d, 1.5d),
            Arrays.asList("cloth", TOTAL, 30d, 10d),
            Arrays.asList("cloth", MAIN_SCENARIO_NAME, null, 10d),
            Arrays.asList("cloth", "s1", null, 10d),
            Arrays.asList("cloth", "s2", null, 10d),
            Arrays.asList("drink", TOTAL, 7.5d, 1.5d),
            Arrays.asList("drink", MAIN_SCENARIO_NAME, null, 2d),
            Arrays.asList("drink", "s1", null, 4d),
            Arrays.asList("drink", "s2", null, 1.5d),
            Arrays.asList("food", TOTAL, 9d, 3d),
            Arrays.asList("food", MAIN_SCENARIO_NAME, null, 3d),
            Arrays.asList("food", "s1", null, 3d),
            Arrays.asList("food", "s2", null, 3d));
  }

  @Test
  void testMergeWithComparators() {
    QueryDto query1 = Query
            .from(this.storeName)
            .select(List.of("category"), List.of(sum("p_sum", "price")))
            .rollup(List.of("category"))
            .orderBy("category", OrderKeywordDto.DESC)
            .build();

    QueryDto query2 = Query
            .from(this.storeName)
            .select(List.of("category", SCENARIO_FIELD_NAME), List.of(min("p_min", "price")))
            .rollup(List.of("category", SCENARIO_FIELD_NAME))
            .orderBy("category", ASC)
            .orderBy(SCENARIO_FIELD_NAME, List.of("s1", MAIN_SCENARIO_NAME, "s2"))
            .build();

    Table result = this.executor.execute(query1, query2, null);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList(GRAND_TOTAL, GRAND_TOTAL, 46.5d, 1.5d),
            Arrays.asList("food", TOTAL, 9d, 3d),
            Arrays.asList("food", "s1", null, 3d),
            Arrays.asList("food", MAIN_SCENARIO_NAME, null, 3d),
            Arrays.asList("food", "s2", null, 3d),
            Arrays.asList("drink", TOTAL, 7.5d, 1.5d),
            Arrays.asList("drink", "s1", null, 4d),
            Arrays.asList("drink", MAIN_SCENARIO_NAME, null, 2d),
            Arrays.asList("drink", "s2", null, 1.5d),
            Arrays.asList("cloth", TOTAL, 30d, 10d),
            Arrays.asList("cloth", "s1", null, 10d),
            Arrays.asList("cloth", MAIN_SCENARIO_NAME, null, 10d),
            Arrays.asList("cloth", "s2", null, 10d));
  }

  @Test
  void testMergeWithColumnSetsPreserveOrder() {
    BucketColumnSetDto group1 = new BucketColumnSetDto("group1", "category")
            .withNewBucket("Food & Drink", List.of("food", "drink"))
            .withNewBucket("Other", List.of("cloth"));
    /*
      +--------------+----------+----------+-------+
      |       group1 | category | category | p_sum |
      +--------------+----------+----------+-------+
      | Food & Drink |     food |     food |   9.0 |
      | Food & Drink |    drink |    drink |   7.5 |
      |        Other |    cloth |    cloth |  30.0 |
      +--------------+----------+----------+-------+
     */
    QueryDto query1 = Query
            .from(this.storeName)
            .select_(List.of(group1), List.of(sum("p_sum", "price")))
            .build();

    BucketColumnSetDto group2 = new BucketColumnSetDto("group2", "subcategory")
            .withNewBucket("Categorized", List.of("biscuit"))
            .withNewBucket("Not categorized", Arrays.asList(null, "other"));
    /*
      +-----------------+-------------+-------+
      |          group2 | subcategory | p_avg |
      +-----------------+-------------+-------+
      |     Categorized |     biscuit |   3.0 |
      | Not categorized |        null |  6.25 |
      +-----------------+-------------+-------+
     */
    QueryDto query2 = Query
            .from(this.storeName)
            .select_(List.of(group2), List.of(avg("p_avg", "price")))
            .build();

    Table result = this.executor.execute(query1, query2, null);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("Food & Drink", "food", TOTAL, TOTAL, 9d, null),
            Arrays.asList("Food & Drink", "drink", TOTAL, TOTAL, 7.5d, null),
            Arrays.asList("Other", "cloth", TOTAL, TOTAL, 30d, null),
            Arrays.asList(TOTAL, TOTAL, "Categorized", "biscuit", null, 3d),
            Arrays.asList(TOTAL, TOTAL, "Not categorized", null, null, 6.25d));
  }

  @Test
  void testQueryLimit() {
    int limit = 2;
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(SCENARIO_FIELD_NAME), List.of(sum("p", "price")))
            .limit(limit)
            .build();
    Table result = this.executor.execute(query);
    Assertions.assertThat(result.count()).isEqualTo(limit); // we don't care about the result, and we can't know what lines will be returned
  }

  @Test
  void testHavingConditions() {
    BasicMeasure price_sum = (BasicMeasure) sum("p", "price");
    BasicMeasure price_sum_expr = new ExpressionMeasure("p_expr", "sum(price)");
    // Single condition
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("ean"), List.of(price_sum))
            .having(criterion(price_sum, ge(9.0)))
            .build();
    Table table = this.executor.execute(query);
    Assertions.assertThat(table).containsExactly(
            List.of("cookie", 9.0d),
            List.of("shirt", 30d));

    // Multiple conditions
    query = Query
            .from(this.storeName)
            .select(List.of("ean"), List.of(price_sum, price_sum_expr))
            .having(all(
                    criterion(price_sum, ge(7d)),
                    criterion(price_sum_expr, le(10d))))
            .build();
    table = this.executor.execute(query);
    Assertions.assertThat(table).containsExactly(
            List.of("bottle", 7.5d, 7.5d),
            List.of("cookie", 9.0d, 9.0d));
  }
}
