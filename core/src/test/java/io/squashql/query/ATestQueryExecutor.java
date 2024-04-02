package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.compiled.CompiledExpressionMeasure;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.*;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.squashql.query.Functions.*;
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;
import static io.squashql.query.dto.OrderKeywordDto.ASC;
import static io.squashql.query.dto.OrderKeywordDto.DESC;
import static io.squashql.transaction.DataLoader.MAIN_SCENARIO_NAME;
import static io.squashql.transaction.DataLoader.SCENARIO_FIELD_NAME;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestQueryExecutor extends ABaseTestQuery {

  protected String storeName = "store" + getClass().getSimpleName().toLowerCase();

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField ean = new TableTypedField(this.storeName, "ean", String.class);
    TableTypedField eanId = new TableTypedField(this.storeName, "eanId", int.class);
    TableTypedField category = new TableTypedField(this.storeName, "category", String.class);
    TableTypedField subcategory = new TableTypedField(this.storeName, "subcategory", String.class);
    TableTypedField price = new TableTypedField(this.storeName, "price", double.class);
    TableTypedField qty = new TableTypedField(this.storeName, "quantity", int.class);
    TableTypedField isFood = new TableTypedField(this.storeName, "isFood", boolean.class);
    return Map.of(this.storeName, List.of(eanId, ean, category, subcategory, price, qty, isFood));
  }

  @Override
  protected void loadData() {
    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{0, "starbuck's coffee", "drink", null, 2d, 10, true}, // use a string with '
            new Object[]{1, "cookie", "food", "biscuit", 3d, 20, true},
            new Object[]{2, "shirt", "cloth", null, 10d, 3, false}
    ));

    this.tm.load("s1", this.storeName, List.of(
            new Object[]{0, "starbuck's coffee", "drink", null, 4d, 10, true},
            new Object[]{1, "cookie", "food", "biscuit", 3d, 20, true},
            new Object[]{2, "shirt", "cloth", null, 10d, 3, false}
    ));

    this.tm.load("s2", this.storeName, List.of(
            new Object[]{0, "starbuck's coffee", "drink", null, 1.5d, 10, true},
            new Object[]{1, "cookie", "food", "biscuit", 3d, 20, true},
            new Object[]{2, "shirt", "cloth", null, 10d, 3, false}
    ));
  }

  @Test
  void testQueryWildcard() {
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of(SCENARIO_FIELD_NAME)), List.of(sum("p", "price"), sum("q", "quantity")))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of(MAIN_SCENARIO_NAME, 15.0d, 33L),
            List.of("s1", 17.0d, 33L),
            List.of("s2", 14.5d, 33L));
  }

  @Test
  void testQueryWildcardWithAliases() {
    Field scenario = tableField(SCENARIO_FIELD_NAME).as("scenario_alias");
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(scenario), List.of(sum("p", "price"), sum("q", "quantity")))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly("scenario_alias", "p", "q");
    Assertions.assertThat(result).containsExactly(
            List.of(MAIN_SCENARIO_NAME, 15.0d, 33L),
            List.of("s1", 17.0d, 33L),
            List.of("s2", 14.5d, 33L));
  }

  @Test
  void testQueryWildcardWithFullRollup() {
    QueryDto query = Query
            .from(this.storeName)
            .where(tableField(SCENARIO_FIELD_NAME), eq(MAIN_SCENARIO_NAME)) // use a filter to have a small output table
            .select(tableFields(List.of(SCENARIO_FIELD_NAME, "category")), List.of(sum("p", "price"), sum("q", "quantity")))
            .rollup(tableFields(List.of(SCENARIO_FIELD_NAME, "category")))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, GRAND_TOTAL, 15d, 33L),
            List.of(MAIN_SCENARIO_NAME, TOTAL, 15d, 33L),
            List.of(MAIN_SCENARIO_NAME, "cloth", 10d, 3L),
            List.of(MAIN_SCENARIO_NAME, "drink", 2d, 10L),
            List.of(MAIN_SCENARIO_NAME, "food", 3d, 20L));
  }

  @Test
  void testQueryWildcardWithFullRollupWithAliases() {
    Field scenario = tableField(SCENARIO_FIELD_NAME).as("scenario_alias");
    Field category = tableField("category").as("category_alias");
    QueryDto query = Query
            .from(this.storeName)
            .where(scenario, eq(MAIN_SCENARIO_NAME)) // use a filter to have a small output table
            .select(List.of(scenario, category), List.of(sum("p", "price"), sum("q", "quantity")))
            .rollup(List.of(scenario, category))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly("scenario_alias", "category_alias", "p", "q");
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, GRAND_TOTAL, 15d, 33L),
            List.of(MAIN_SCENARIO_NAME, TOTAL, 15d, 33L),
            List.of(MAIN_SCENARIO_NAME, "cloth", 10d, 3L),
            List.of(MAIN_SCENARIO_NAME, "drink", 2d, 10L),
            List.of(MAIN_SCENARIO_NAME, "food", 3d, 20L));
  }

  /**
   * Same as {@link #testQueryWildcardWithFullRollup()} but name of columns are explicit i.e. full name.
   */
  @Test
  void testQueryWildcardWithFullRollupFullName() {
    QueryDto query = Query
            .from(this.storeName)
            .where(tableField(SqlUtils.getFieldFullName(this.storeName, SCENARIO_FIELD_NAME)), eq(MAIN_SCENARIO_NAME)) // use a filter to have a small output table
            .select(tableFields(List.of(SqlUtils.getFieldFullName(this.storeName, SCENARIO_FIELD_NAME), SqlUtils.getFieldFullName(this.storeName, "category"))), List.of(sum("p", "price"), sum("q", "quantity")))
            .rollup(tableField(SqlUtils.getFieldFullName(this.storeName, SCENARIO_FIELD_NAME)), tableField(SqlUtils.getFieldFullName(this.storeName, "category")))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, GRAND_TOTAL, 15d, 33L),
            List.of(MAIN_SCENARIO_NAME, TOTAL, 15d, 33L),
            List.of(MAIN_SCENARIO_NAME, "cloth", 10d, 3L),
            List.of(MAIN_SCENARIO_NAME, "drink", 2d, 10L),
            List.of(MAIN_SCENARIO_NAME, "food", 3d, 20L));
  }

  @Test
  void testQueryWildcardWithFullRollupOnColumnTypeInt() {
    QueryDto query = Query
            .from(this.storeName)
            .where(tableField(SCENARIO_FIELD_NAME), eq(MAIN_SCENARIO_NAME)) // use a filter to have a small output table
            .select(tableFields(List.of("eanId")), List.of(sum("p", "price"), sum("q", "quantity")))
            .rollup(tableField("eanId"))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, 15d, 33L),
            List.of(translate(0), 2d, 10L),
            List.of(translate(1), 3d, 20L),
            List.of(translate(2), 10d, 3L));
  }

  /**
   * Subcategory is null for some rows. The engine needs to be capable of distinguish null values that are returned by
   * ROLLUP from standard null values.
   */
  @Test
  void testQueryWildcardWithFullRollupWithNullValues() {
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("subcategory")), List.of(sum("q", "quantity")))
            .rollup(tableField("subcategory"))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, 99L),
            List.of("biscuit", 60L),
            Arrays.asList(null, 39L));
  }

  @Test
  void testQueryWildcardPartialRollupWithTwoColumns() {
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of(SCENARIO_FIELD_NAME, "category")), List.of(sum("q", "quantity")))
            .rollup(tableField("category")) // We don't care here about total on scenario
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of(MAIN_SCENARIO_NAME, TOTAL, 33L),
            List.of(MAIN_SCENARIO_NAME, "cloth", 3L),
            List.of(MAIN_SCENARIO_NAME, "drink", 10L),
            List.of(MAIN_SCENARIO_NAME, "food", 20L),
            List.of("s1", TOTAL, 33L),
            List.of("s1", "cloth", 3L),
            List.of("s1", "drink", 10L),
            List.of("s1", "food", 20L),
            List.of("s2", TOTAL, 33L),
            List.of("s2", "cloth", 3L),
            List.of("s2", "drink", 10L),
            List.of("s2", "food", 20L));

    query = Query
            .from(this.storeName)
            .select(tableFields(List.of(SCENARIO_FIELD_NAME, "category")), List.of(sum("q", "quantity")))
            .rollup(tableField(SCENARIO_FIELD_NAME)) // try with another column
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of(TOTAL, "cloth", 9L),
            List.of(TOTAL, "drink", 30L),
            List.of(TOTAL, "food", 60L),
            List.of(MAIN_SCENARIO_NAME, "cloth", 3L),
            List.of(MAIN_SCENARIO_NAME, "drink", 10L),
            List.of(MAIN_SCENARIO_NAME, "food", 20L),
            List.of("s1", "cloth", 3L),
            List.of("s1", "drink", 10L),
            List.of("s1", "food", 20L),
            List.of("s2", "cloth", 3L),
            List.of("s2", "drink", 10L),
            List.of("s2", "food", 20L));
  }

  @Test
  void testQueryWildcardPartialRollupWithThreeColumns() {
    QueryDto query = Query
            .from(this.storeName)
            .where(tableField(SCENARIO_FIELD_NAME), eq("s1")) // filter to reduce output table size
            .select(tableFields(List.of(SCENARIO_FIELD_NAME, "category", "subcategory")), List.of(sum("q", "quantity")))
            .rollup(tableField("category"), tableField("subcategory"))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("s1", TOTAL, TOTAL, 33L),
            Arrays.asList("s1", "cloth", TOTAL, 3L),
            Arrays.asList("s1", "cloth", null, 3L),
            Arrays.asList("s1", "drink", TOTAL, 10L),
            Arrays.asList("s1", "drink", null, 10L),
            Arrays.asList("s1", "food", TOTAL, 20L),
            Arrays.asList("s1", "food", "biscuit", 20L));

    query = Query
            .from(this.storeName)
            .where(tableField(SCENARIO_FIELD_NAME), eq("s1")) // filter to reduce output table size
            .select(tableFields(List.of(SCENARIO_FIELD_NAME, "category", "subcategory")), List.of(sum("q", "quantity")))
            .rollup(tableField("category")) // Only total for category
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("s1", TOTAL, "biscuit", 20L),
            Arrays.asList("s1", TOTAL, null, 13L),
            Arrays.asList("s1", "cloth", null, 3L),
            Arrays.asList("s1", "drink", null, 10L),
            Arrays.asList("s1", "food", "biscuit", 20L));
  }

  @Test
  void testQueryRollupIncorrect() {
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(sum("p", "price")))
            .rollup(tableField("subcategory")) // not correct because it should be a subset of the column in the select
            .build();
    Assertions.assertThatThrownBy(() -> this.executor.executeQuery(query))
            // The columns contain in rollup [`subcategory`] must be a subset of the columns contain in the select [`category`]
            .hasMessageContaining("must be a subset of the columns contain in the select");
  }

  @Test
  void testQueryWildcardCount() {
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of(SCENARIO_FIELD_NAME)), List.of(CountMeasure.INSTANCE))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of(MAIN_SCENARIO_NAME, 3L),
            List.of("s1", 3L),
            List.of("s2", 3L));
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(SCENARIO_FIELD_NAME, CountMeasure.ALIAS);
  }

  @Test
  void testQuerySeveralCoordinates() {
    QueryDto query = Query
            .from(this.storeName)
            .where(tableField(SCENARIO_FIELD_NAME), in("s1", "s2"))
            .select(tableFields(List.of(SCENARIO_FIELD_NAME)), List.of(sum("p", "price"), sum("q", "quantity")))
            .build();
    Table table = this.executor.executeQuery(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(
            List.of("s1", 17.0d, 33L),
            List.of("s2", 14.5d, 33L));
  }

  @Test
  void testQuerySingleCoordinate() {
    QueryDto query = Query
            .from(this.storeName)
            .where(tableField(SCENARIO_FIELD_NAME), eq("s1"))
            .select(tableFields(List.of(SCENARIO_FIELD_NAME)), List.of(sum("p", "price"), sum("q", "quantity")))
            .build();
    Table table = this.executor.executeQuery(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(List.of("s1", 17.0d, 33L));
  }

  @Test
  void testConditions() {
    QueryDto query = Query
            .from(this.storeName)
            .where(Functions.all(
                    criterion("ean", eq("starbuck's coffee")),
                    criterion(SCENARIO_FIELD_NAME, eq(MAIN_SCENARIO_NAME)),
                    criterion("category", in("cloth", "drink"))))
            .select(tableFields(List.of("category", "ean")), List.of(sum("q", "quantity")))
            .build();

    Table table = this.executor.executeQuery(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(List.of("drink", "starbuck's coffee", 10L));

    query = Query
            .from(this.storeName)
            .where(Functions.all(
                    criterion("ean", eq("starbuck's coffee")),
                    criterion(SCENARIO_FIELD_NAME, eq(MAIN_SCENARIO_NAME)),
                    criterion("category", in("cloth", "drink")),
                    criterion("quantity", Functions.gt(10))))
            .select(tableFields(List.of("category", "ean")), List.of(sum("q", "quantity")))
            .build();
    table = this.executor.executeQuery(query);
    Assertions.assertThat(table).isEmpty();
  }

  @Test
  void testConditionsNullNotNull() {
    QueryDto query = Query.from(this.storeName)
            .where(criterion("subcategory", isNotNull()))
            .select(tableFields(List.of("ean")), List.of(CountMeasure.INSTANCE))
            .build();
    Table table = this.executor.executeQuery(query);
    Assertions.assertThat(table).containsExactly(List.of("cookie", 3L));

    query = Query.from(this.storeName)
            .where(criterion("subcategory", isNull()))
            .select(tableFields(List.of("ean")), List.of(CountMeasure.INSTANCE))
            .build();
    table = this.executor.executeQuery(query);
    Assertions.assertThat(table).containsExactly(
            List.of("shirt", 3L),
            List.of("starbuck's coffee", 3L));
  }

  @Test
  void testLikeCondition() {
    QueryDto query = Query
            .from(this.storeName)
            .where(tableField(SCENARIO_FIELD_NAME), Functions.like("s%")) // start with s
            .select(tableFields(List.of(SCENARIO_FIELD_NAME)), List.of())
            .build();
    Table table = this.executor.executeQuery(query);
    Assertions.assertThat(table).containsExactly(List.of("s1"), List.of("s2"));
  }

  @Test
  void testBooleanCondition() {
    QueryDto query = Query
            .from(this.storeName)
            .where(criterion("isFood", eq(true)))
            .select(tableFields(List.of("ean")), List.of())
            .build();
    Table table = this.executor.executeQuery(query);
    Assertions.assertThat(table).containsExactly(List.of("cookie"), List.of("starbuck's coffee"));
  }

  @Test
  void testConditionAliasedField() {
    Field scenario = tableField(SCENARIO_FIELD_NAME).as("scenario_aliased");
    QueryDto query = Query
            .from(this.storeName)
            .where(criterion(scenario, eq("s1")))
            .select(List.of(scenario), List.of(CountMeasure.INSTANCE))
            .build();
    Table table = this.executor.executeQuery(query);
    Assertions.assertThat(table).containsExactly(List.of("s1", 3L));
  }

  /**
   * Without measure, we can use it to do a discovery.
   */
  @Test
  void testDiscovery() {
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of(SCENARIO_FIELD_NAME)), List.of())
            .build();
    Table table = this.executor.executeQuery(query);
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
    QueryRewriter qr = this.queryEngine.queryRewriter(null);
    String expression = String.format("sum(case when %s = 'food' OR %s = 'drink' then %s end)",
            qr.fieldName("category"),
            qr.fieldName("category"),
            qr.fieldName("quantity"));
    ConditionDto or = or(eq("food"), eq("drink"));
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of(SCENARIO_FIELD_NAME)),
                    List.of(new ExpressionMeasure("quantity if food or drink", expression),
                            sumIf("quantity filtered", this.storeName + ".quantity", criterion(this.storeName + ".category", or))))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of(MAIN_SCENARIO_NAME, 30L, 30L),
            List.of("s1", 30L, 30L),
            List.of("s2", 30L, 30L));
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(SCENARIO_FIELD_NAME, "quantity if food or drink", "quantity filtered");

    // Multiple fields
    CriteriaDto category = criterion("category", or);
    query = Query
            .from(this.storeName)
            .select(tableFields(List.of(SCENARIO_FIELD_NAME)),
                    List.of(sumIf("quantity filtered", "quantity", Functions.all(category, criterion("subcategory", eq("biscuit"))))))
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of(MAIN_SCENARIO_NAME, 20L),
            List.of("s1", 20L),
            List.of("s2", 20L));
  }

  @Test
  void testSumIfWithFullPath() {
    ConditionDto or = or(eq("food"), eq("drink"));
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of(SCENARIO_FIELD_NAME)),
                    List.of(sumIf("quantity filtered", this.storeName + ".quantity", criterion(this.storeName + ".category", or))))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of(MAIN_SCENARIO_NAME, 30L),
            List.of("s1", 30L),
            List.of("s2", 30L));
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(SCENARIO_FIELD_NAME, "quantity filtered");
  }

  /**
   * {@code countDistinct(category)}
   */
  @Test
  void testCountDistinct() {
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")),
                    List.of(
                            count("count categories", this.storeName + ".category"),
                            countDistinct("count distinct categories", this.storeName + ".category")))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("drink", 3L, 1L),
            List.of("food", 3L, 1L),
            List.of("cloth", 3L, 1L));
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly("category", "count categories", "count distinct categories");
  }

  /**
   * {@code countDistinctIf(category, isFood = 'TRUE')}
   */
  @Test
  void testCountDistinctIf() {
    QueryDto query = Query
            .from(this.storeName)
            .select(Collections.emptyList(),
                    List.of(
                            countIf("count categories if food", this.storeName + ".category",
                                    criterion(new TableField(this.storeName + ".isFood"), eq(true))),
                            countDistinctIf("count distinct categories if food", this.storeName + ".category",
                                    criterion(new TableField(this.storeName + ".isFood"), eq(true)))))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(6L, 2L));
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly("count categories if food", "count distinct categories if food");
  }

  @Test
  void testOrderByColumn() {
    QueryDto query = Query
            .from(this.storeName)
            .where(tableField("category"), in("cloth", "drink"))
            .select(tableFields(List.of(SCENARIO_FIELD_NAME, "category")), List.of(CountMeasure.INSTANCE))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of(MAIN_SCENARIO_NAME, "cloth", 1L),
            List.of(MAIN_SCENARIO_NAME, "drink", 1L),
            List.of("s1", "cloth", 1L),
            List.of("s1", "drink", 1L),
            List.of("s2", "cloth", 1L),
            List.of("s2", "drink", 1L));

    query.orderBy(tableField("category"), DESC);
    result = this.executor.executeQuery(query);
    // The order is enforced by the user on category, and natural order applies to other columns
    Assertions.assertThat(result).containsExactly(
            List.of(MAIN_SCENARIO_NAME, "drink", 1L),
            List.of("s1", "drink", 1L),
            List.of("s2", "drink", 1L),
            List.of(MAIN_SCENARIO_NAME, "cloth", 1L),
            List.of("s1", "cloth", 1L),
            List.of("s2", "cloth", 1L));

    List<String> elements = List.of("s2", MAIN_SCENARIO_NAME, "s1");
    query.orderBy(tableField(SCENARIO_FIELD_NAME), elements);
    result = this.executor.executeQuery(query);
    // Order of orderBy is important. First orderBy => category in DESC order, then orderBy scenario in the order defined by the user
    Assertions.assertThat(result).containsExactly(
            List.of("s2", "drink", 1L),
            List.of(MAIN_SCENARIO_NAME, "drink", 1L),
            List.of("s1", "drink", 1L),
            List.of("s2", "cloth", 1L),
            List.of(MAIN_SCENARIO_NAME, "cloth", 1L),
            List.of("s1", "cloth", 1L));
  }

  @Test
  void testOrderByWithRollup() {
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(sum("p_sum", "price")))
            .rollup(tableFields(List.of("category")))
            .orderBy(tableField("category"), ASC)
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, 46.5d),
            List.of("cloth", 30d),
            List.of("drink", 7.5d),
            List.of("food", 9d));

    query.orderBy(tableField("category"), DESC);
    result = this.executor.executeQuery(query);
    // Total AND Grand Total always on top
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, 46.5d),
            List.of("food", 9d),
            List.of("drink", 7.5d),
            List.of("cloth", 30d));

    // With explicit ordering
    query.orderBy(tableField("category"), List.of("drink", "food", "cloth"));
    result = this.executor.executeQuery(query);
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
            .select(tableFields(List.of("subcategory")), List.of(CountMeasure.INSTANCE))
            .build();
    Table result = this.executor.executeQuery(query);
    // Without explicit ordering, null comes last because of: Comparator.nullsLast(Comparator.naturalOrder())
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("biscuit", 3L),
            Arrays.asList(null, 6L));

    // With explicit ordering
    query = Query
            .from(this.storeName)
            .select(tableFields(List.of("subcategory")), List.of(CountMeasure.INSTANCE))
            .orderBy(tableField("subcategory"), ASC)
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("biscuit", 3L),
            Arrays.asList(null, 6L));
  }

  @Test
  void testOrderByMeasure() {
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(sum("p", "price")))
            .build();
    Table result = this.executor.executeQuery(query);
    // Default order
    Assertions.assertThat(result).containsExactly(
            List.of("cloth", 30d),
            List.of("drink", 7.5d),
            List.of("food", 9d));

    // With AliasedField
    query = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(sum("p", "price")))
            .orderBy(new AliasedField("p"), ASC)
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of("drink", 7.5d),
            List.of("food", 9d),
            List.of("cloth", 30d));
  }

  @Test
  void testConstantMeasures() {
    Measure integer = Functions.integer(100);
    Measure decimal = Functions.decimal(100.5);
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
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(List.of(4650d, 4673.25d, 9900L, 9949.5d, translate(100), 100.5));
    Assertions.assertThat(result.headers().stream().map(Header::name).toList())
            .containsExactly("a1", "a2", "b1", "b2", "constant(100)", "constant(100.5)");
  }

  @Test
  void testRawQueryExecution() {
    QueryRewriter qr = this.executor.queryEngine.queryRewriter(null);
    String tableName = qr.tableName(this.storeName);
    String ean = qr.fieldName("ean");
    String price = qr.fieldName("price");
    // Use SUMPRICE in upper case to simplify the test. Indeed, Snowflake converts lower case aliases to upper case...
    Table result = this.executor.executeRaw(String.format("select %s, sum(%s) as SUMPRICE from %s group by %s order by %s", ean, price, tableName, ean, ean));
    Assertions.assertThat(result.headers().stream().map(header -> header.name()).toList())
            .containsExactly("ean", "SUMPRICE");
    Assertions.assertThat(result).containsExactly(
            List.of("cookie", 9.0d),
            List.of("shirt", 30d),
            List.of("starbuck's coffee", 7.5d));
  }

  @Test
  void testMergeTables() {
    QueryDto query1 = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(sum("p_sum", "price")))
            .rollup(tableFields(List.of("category")))
            .build();

    QueryDto query2 = Query
            .from(this.storeName)
            .select(tableFields(List.of("category", SCENARIO_FIELD_NAME)), List.of(min("p_min", "price")))
            .rollup(tableFields(List.of("category", SCENARIO_FIELD_NAME)))
            .build();

    Table result = this.executor.executeQueryMerge(QueryMergeDto.from(query1).join(query2, JoinType.FULL), null);

    Assertions.assertThat(result.headers().stream().map(Header::name).toList())
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
  void testMergeThreeTables() {
    QueryDto query1 = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(sum("p_sum", "price")))
            .build();

    QueryDto query2 = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(min("p_min", "price")))
            .build();

    QueryDto query3 = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(max("p_max", "price")))
            .build();

    QueryMergeDto queryMerge = QueryMergeDto.from(query1)
            .join(query2, JoinType.INNER)
            .join(query3, JoinType.INNER);
    Table result = this.executor.executeQueryMerge(queryMerge, null);
    Assertions.assertThat(result.headers().stream().map(Header::name).toList())
            .containsExactly("category", "p_sum", "p_min", "p_max");
    Assertions.assertThat(result).containsExactly(
            List.of("cloth", 30d, 10d, 10d),
            List.of("drink", 7.5d, 1.5d, 4d),
            List.of("food", 9d, 3d, 3d));
  }

  @Test
  void testMergeWithComparators() {
    QueryDto query1 = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(sum("p_sum", "price")))
            .rollup(tableFields(List.of("category")))
            .orderBy(tableField("category"), DESC)
            .build();

    QueryDto query2 = Query
            .from(this.storeName)
            .select(tableFields(List.of("category", SCENARIO_FIELD_NAME)), List.of(min("p_min", "price")))
            .rollup(tableFields(List.of("category", SCENARIO_FIELD_NAME)))
            .orderBy(tableField("category"), ASC)
            .orderBy(tableField(SCENARIO_FIELD_NAME), List.of("s1", MAIN_SCENARIO_NAME, "s2"))
            .build();

    Table result = this.executor.executeQueryMerge(QueryMergeDto.from(query1).join(query2, JoinType.FULL), null);
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
    GroupColumnSetDto group1 = new GroupColumnSetDto("group1", tableField("category"))
            .withNewGroup("Food & Drink", List.of("food", "drink"))
            .withNewGroup("Other", List.of("cloth"));
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

    GroupColumnSetDto group2 = new GroupColumnSetDto("group2", tableField("subcategory"))
            .withNewGroup("Categorized", List.of("biscuit"))
            .withNewGroup("Not categorized", Arrays.asList(null, "other"));
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

    Table result = this.executor.executeQueryMerge(QueryMergeDto.from(query1).join(query2, JoinType.FULL), null);
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
            .select(tableFields(List.of(SCENARIO_FIELD_NAME)), List.of(sum("p", "price")))
            .limit(limit)
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.count()).isEqualTo(limit); // we don't care about the result, and we can't know what lines will be returned
  }

  @Test
  void testTotalCountWithQueryLimit() {
    int limit = 1;
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(CountMeasure.INSTANCE, TotalCountMeasure.INSTANCE))
            .limit(limit)
            .build();

    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.count()).isEqualTo(limit); // we don't care about the result, and we can't know what lines will be returned
    Assertions.assertThat(result.getAggregateValues(CompiledExpressionMeasure.COMPILED_TOTAL_COUNT).get(0)).isEqualTo(3L);
  }

  @Test
  void testHavingConditions() {
    BasicMeasure price_sum = (BasicMeasure) sum("pricesum", "price");
    BasicMeasure price_sum_expr = new ExpressionMeasure("p_expr", "sum(" + this.queryEngine.queryRewriter(null).fieldName("price") + ")");
    // Single condition
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("ean")), List.of(price_sum))
            .having(criterion(price_sum, ge(9.0)))
            .build();
    Table table = this.executor.executeQuery(query);
    Assertions.assertThat(table).containsExactly(
            List.of("cookie", 9.0d),
            List.of("shirt", 30d));

    // Multiple conditions
    query = Query
            .from(this.storeName)
            .select(tableFields(List.of("ean")), List.of(price_sum, price_sum_expr))
            .having(all(
                    criterion(price_sum, ge(7d)),
                    criterion(price_sum_expr, le(10d))))
            .build();
    table = this.executor.executeQuery(query);
    Assertions.assertThat(table).containsExactly(
            List.of("cookie", 9.0d, 9.0d),
            List.of("starbuck's coffee", 7.5d, 7.5d));
  }
}
