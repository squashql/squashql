package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.agg.AggregationFunction;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.*;
import io.squashql.query.parameter.QueryCacheParameter;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import io.squashql.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.squashql.query.Functions.*;
import static io.squashql.query.QueryExecutor.createPivotTableContext;
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;
import static io.squashql.transaction.DataLoader.SCENARIO_FIELD_NAME;

@TestClass(ignore = {TestClass.Type.BIGQUERY, TestClass.Type.SNOWFLAKE, TestClass.Type.CLICKHOUSE, TestClass.Type.SPARK, TestClass.Type.POSTGRESQL})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestQueryCache extends ABaseTestQuery {

  protected String storeName = "store" + getClass().getSimpleName().toLowerCase();
  protected String competitorStoreName = "competitor" + getClass().getSimpleName().toLowerCase();
  protected String other = "other" + getClass().getSimpleName().toLowerCase();
  protected GlobalCache queryCache;

  @Override
  protected void afterSetup() {
    this.queryCache = (GlobalCache) this.executor.queryCache;
  }

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField scenario = new TableTypedField(this.storeName, "scenario", String.class);
    TableTypedField ean = new TableTypedField(this.storeName, "ean", String.class);
    TableTypedField category = new TableTypedField(this.storeName, "category", String.class);
    TableTypedField price = new TableTypedField(this.storeName, "price", double.class);
    TableTypedField qty = new TableTypedField(this.storeName, "quantity", int.class);

    TableTypedField comp_ean = new TableTypedField(this.competitorStoreName, "comp_ean", String.class);
    TableTypedField comp_name = new TableTypedField(this.competitorStoreName, "comp_name", String.class);
    TableTypedField comp_price = new TableTypedField(this.competitorStoreName, "comp_price", double.class);

    return Map.of(
            this.storeName, List.of(scenario, ean, category, price, qty),
            this.competitorStoreName, List.of(comp_ean, comp_name, comp_price),
            this.other, List.of(ean, category, price)
    );
  }

  @Override
  protected void loadData() {
    this.tm.load(this.storeName, List.of(
            new Object[]{"base", "bottle", "drink", 2d, 10},
            new Object[]{"base", "cookie", "food", 3d, 20},
            new Object[]{"base", "shirt", "cloth", 10d, 3}
    ));

    this.tm.load(this.competitorStoreName, List.of(
            new Object[]{"bottle", "A", 2d},
            new Object[]{"bottle", "B", 1d},
            new Object[]{"cookie", "A", 3d},
            new Object[]{"cookie", "B", 2d},
            new Object[]{"shirt", "A", 10d},
            new Object[]{"shirt", "B", 9d}
    ));

    this.tm.load(this.other, List.of(
            new Object[]{"bottle", "drink", 2d},
            new Object[]{"cookie", "food", 3d},
            new Object[]{"shirt", "cloth", 10d},
            new Object[]{"other", null, 5d} // use null
    ));
  }

  @BeforeEach
  void beforeEach() {
    this.queryCache.clear();
  }

  @Test
  void testQuerySameColumns() {
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of(SCENARIO_FIELD_NAME)), List.of(sum("ps", "price"), sum("qs", "quantity")))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d, 33L));
    assertCacheStats(0, 3);

    // Execute the same
    result = this.executor.executeQuery(query);
    assertCacheStats(3, 3);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d, 33L));

    // New query but same columns, same measure
    query = Query
            .from(this.storeName)
            .select(tableFields(List.of(SCENARIO_FIELD_NAME)), List.of(sum("qs", "quantity")))
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 33L));
    assertCacheStats(5, 3);
  }

  @Test
  void testQueryDifferentColumns() {
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of(SCENARIO_FIELD_NAME)), List.of(sum("ps", "price")))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d));
    assertCacheStats(0, 2);

    // remove column, no column.
    query = Query
            .from(this.storeName)
            .select(List.of(), List.of(sum("ps", "price")))
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(15.0d));
    assertCacheStats(0, 4);

    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(15.0d));
    assertCacheStats(2, 4);
  }

  @Test
  void testQueryDifferentConditions() {
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(sum("ps", "price")))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("drink", 2d),
            List.of("cloth", 10d),
            List.of("food", 3d));
    assertCacheStats(0, 2);

    query = Query
            .from(this.storeName)
            .where(tableField("category"), eq("drink"))
            .select(tableFields(List.of("category")), List.of(sum("ps", "price")))
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("drink", 2d));
    assertCacheStats(0, 4);

    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("drink", 2d));
    assertCacheStats(2, 4);

    query = Query
            .from(this.storeName)
            .where(tableField("category"), in("food", "cloth"))
            .select(tableFields(List.of("category")), List.of(sum("ps", "price")))
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("cloth", 10d),
            List.of("food", 3d));
    assertCacheStats(2, 6);
  }

  @Test
  void testQueryWithJoin() {
    QueryDto query = Query
            .from(this.storeName)
            .join(competitorStoreName, JoinType.INNER)
            .on(criterion(this.storeName + ".ean", this.competitorStoreName + ".comp_ean", ConditionType.EQ))
            .select(tableFields(List.of("category")), List.of(sum("ps", "price")))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("drink", 4d), // value are doubled because of the join
            List.of("cloth", 20d),
            List.of("food", 6d));
    assertCacheStats(0, 2);

    // Same query but wo. join. Should not hit the cache
    query = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(sum("ps", "price")))
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("drink", 2d),
            List.of("cloth", 10d),
            List.of("food", 3d));
    assertCacheStats(0, 4);
  }

  @Test
  void testWithSubQuery() {
    Measure ca = sum("ca", "price");
    Measure sum_ca = sum("sum_ca", "ca");
    Measure avg_ca = avg("mean_ca", "ca");
    Measure min = min("ca", "price");

    QueryDto firstSubQuery = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(ca)) // ca per category
            .build();

    QueryDto queryDto = Query
            .from(firstSubQuery)
            .select(List.of(), List.of(avg_ca)) // avg of ca
            .build();
    Table result = this.executor.executeQuery(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(5d));
    assertCacheStats(0, 2);

    // Change the sub query
    QueryDto secondSubQuery = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(min)) // change agg function
            .build();
    queryDto = Query
            .from(secondSubQuery)
            .select(List.of(), List.of(avg_ca)) // avg of ca
            .build();
    result = this.executor.executeQuery(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(5d));
    assertCacheStats(0, 4);

    // Change again the sub query
    secondSubQuery = Query
            .from(this.storeName)
            .select(tableFields(List.of("ean")), List.of(min)) // change here
            .build();
    queryDto = Query
            .from(secondSubQuery)
            .select(List.of(), List.of(avg_ca)) // avg of ca
            .build();
    result = this.executor.executeQuery(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(5d));
    assertCacheStats(0, 6);

    // Hit the cache
    queryDto = Query
            .from(firstSubQuery) // same first sub-query
            .select(List.of(), List.of(avg_ca, sum_ca)) // ask for another measure
            .build();
    result = this.executor.executeQuery(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(5d, 15d));
    assertCacheStats(2, 7); // avg_ca and count hit the cache
  }

  @Test
  void testQueryWithSubQueryAndColumnsAndConditions() {
    QueryDto firstSubQuery = Query
            .from(this.storeName)
            .select(tableFields(List.of("category", "ean")), List.of(sum("ca", "price"))) // ca per scenario
            .build();

    QueryDto queryDto = Query
            .from(firstSubQuery)
            .select(tableFields(List.of("ean")), List.of(avg("mean", "ca"))) // ean needs to be in the subquery to make it work!
            .build();
    Table result = this.executor.executeQuery(queryDto);
    Assertions.assertThat(result).containsExactly(
            List.of("bottle", 2d),
            List.of("cookie", 3d),
            List.of("shirt", 10d));
  }

  @Test
  void testUseCacheParameter() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(), List.of(sum("ps", "price")))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(15d));
    assertCacheStats(0, 2);

    // Execute the same
    result = this.executor.executeQuery(query.withParameter(QueryCacheParameter.KEY, new QueryCacheParameter(QueryCacheParameter.Action.NOT_USE)));
    // No cache so no hitCount, no missCount changes
    assertCacheStats(0, 2);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(15d));

    query.withParameter(QueryCacheParameter.KEY, new QueryCacheParameter(QueryCacheParameter.Action.USE));
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(15d));
    assertCacheStats(2, 2);

    query.withParameter(QueryCacheParameter.KEY, new QueryCacheParameter(QueryCacheParameter.Action.INVALIDATE));
    // Invalidate should empty the cache and fill it with new values.
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(15d));
    assertCacheStats(0, 2);
  }

  @Test
  void testEvictionMaxSize() throws InterruptedException {
    AtomicInteger c = new AtomicInteger();
    CountDownLatch latch = new CountDownLatch(1);
    // Use a cache with a small size and a listener because removal is asynchronous.
    CaffeineQueryCache cache = new CaffeineQueryCache(2, (key, value, cause) -> {
      c.getAndIncrement();
      latch.countDown();
    });
    QueryExecutor executor = new QueryExecutor(this.createQueryEngine(this.datastore), new GlobalCache(() -> cache));

    Supplier<QueryDto> querySupplier = () -> new QueryDto()
            .table(this.storeName)
            .withColumn(tableField(SCENARIO_FIELD_NAME))
            .withMeasure(new AggregatedMeasure("ps", "price", AggregationFunction.SUM));
    // Scope 1 added to the cache
    executor.executeQuery(querySupplier.get());
    TestUtil.assertCacheStats(cache.stats(), 0, 2);

    // Scope 2 added to the cache
    executor.executeQuery(querySupplier.get().withColumn(tableField("category")));
    TestUtil.assertCacheStats(cache.stats(), 0, 4);

    // Scope 3, should evict an entry in the cache
    executor.executeQuery(querySupplier.get().withCondition(tableField("category"), Functions.eq("drink")));
    latch.await(60, TimeUnit.SECONDS);
    CacheStatsDto stats = cache.stats();
    TestUtil.assertCacheStats(cache.stats(), 0, 6);
    Assertions.assertThat(c.getAndIncrement()).isEqualTo(1);
    Assertions.assertThat(stats.evictionCount).isEqualTo(1);
  }

  @Test
  void testQueryDifferentUsers() {
    BasicUser paul = new BasicUser("paul");
    BasicUser peter = new BasicUser("peter");

    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of(SCENARIO_FIELD_NAME)), List.of(sum("ps", "price"), sum("qs", "quantity")))
            .build();
    Table result = execute(this.executor, query, paul);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d, 33L));
    assertCacheStats(0, 3, paul);
    assertCacheStats(0, 0, peter);

    // Execute the same query, same user
    result = execute(this.executor, query, paul);
    assertCacheStats(3, 3, paul);
    assertCacheStats(0, 0, peter);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d, 33L));

    // Execute the same query, but different user
    result = execute(this.executor, query, peter);
    assertCacheStats(0, 3, peter);
    assertCacheStats(3, 3, paul);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d, 33L));

    /// Test invalid cache for a given user
    query.withParameter(QueryCacheParameter.KEY, new QueryCacheParameter(QueryCacheParameter.Action.INVALIDATE));
    execute(this.executor, query, paul);
    assertCacheStats(0, 3, paul); // reset for paul
    assertCacheStats(0, 3, peter); // same for peter
  }

  @Test
  void testQueryDifferentUsersWithSubQuery() {
    BasicUser paul = new BasicUser("paul");
    BasicUser peter = new BasicUser("peter");

    Measure ca = sum("ca", "price");
    Measure avg_ca = avg("mean_ca", "ca");

    QueryDto subQuery = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(ca)) // ca per category
            .build();

    QueryDto queryDto = Query
            .from(subQuery)
            .select(List.of(), List.of(avg_ca)) // avg of ca
            .build();
    Table result = execute(this.executor, queryDto, paul);
    Assertions.assertThat(result).containsExactly(List.of(5d));
    assertCacheStats(0, 2, paul);

    // Same query, same user
    execute(this.executor, queryDto, paul);
    assertCacheStats(2, 2, paul);

    // Same query, different user
    execute(this.executor, queryDto, peter);
    assertCacheStats(0, 2, peter);
  }

  @Test
  void testQueryDifferentLimits() {
    // No limit, it will use the default value (10k)
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("ean")), List.of(sum("ps", "price")))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("bottle", 2d),
            List.of("cookie", 3d),
            List.of("shirt", 10d));
    assertCacheStats(0, 2);

    int limit = 2;
    query.withLimit(limit);
    result = this.executor.executeQuery(query);
    assertCacheStats(0, 4);
    Assertions.assertThat(result.count()).isEqualTo(limit);

    // Same with same limit
    result = this.executor.executeQuery(query);
    assertCacheStats(2, 4);
    Assertions.assertThat(result.count()).isEqualTo(limit);

    // Same with different limit
    int otherLimit = 1;
    query.withLimit(otherLimit);
    result = this.executor.executeQuery(query);
    assertCacheStats(2, 6);
    Assertions.assertThat(result.count()).isEqualTo(otherLimit);
  }

  @Test
  void testWithDifferentCte() {
    // Clickhouse does not support non-equi join.
    Assumptions.assumeFalse(this.queryEngine.getClass().getSimpleName().contains(TestClass.Type.CLICKHOUSE.className));

    VirtualTableDto cte = new VirtualTableDto("cte",
            List.of("min", "max", "bucket"),
            List.of(List.of(0d, 5d, "cheap"), List.of(5d, 100d, "notcheap")));
    QueryDto query = Query
            .from(this.storeName)
            .join(cte, JoinType.INNER)
            .on(all(criterion("price", "min", ConditionType.GE),
                    criterion("price", "max", ConditionType.LT)))
            .select(tableFields(List.of("ean", "bucket")), List.of())
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of("bottle", "cheap"),
            List.of("cookie", "cheap"),
            List.of("shirt", "notcheap"));
    assertCacheStats(0, 1);

    this.executor.executeQuery(query);
    assertCacheStats(1, 1);

    // Change cte
    cte = new VirtualTableDto("cte",
            List.of("min", "max", "bucket"),
            List.of(List.of(0d, 3d, "cheap"), List.of(3d, 100d, "notcheap")));
    query = Query
            .from(this.storeName)
            .join(cte, JoinType.INNER)
            .on(all(criterion("price", "min", ConditionType.GE),
                    criterion("price", "max", ConditionType.LT)))
            .select(tableFields(List.of("ean", "bucket")), List.of())
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of("bottle", "cheap"),
            List.of("cookie", "notcheap"),
            List.of("shirt", "notcheap"));
    assertCacheStats(1, 2);
  }

  @Test
  void testCacheWithFullPath() {
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of(this.storeName + "." + SCENARIO_FIELD_NAME)), List.of())
            .build();
    this.executor.executeQuery(query);
    assertCacheStats(0, 1);

    query = Query
            .from(this.storeName)
            .select(tableFields(List.of(this.storeName + "." + SCENARIO_FIELD_NAME)), List.of())
            .build();
    this.executor.executeQuery(query);
    assertCacheStats(1, 1);
  }

  @Test
  void testQueryPivotTable() {
    final Field category = tableField("category");
    final Field ean = tableField("ean");
    QueryDto q = Query
            .from(this.storeName)
            .select(List.of(category, ean), List.of(sum("ca", "price")))
            .build();
    int base = 0;
    this.executor.executePivotQuery(new PivotTableQueryDto(q, List.of(category), List.of(ean)));
    assertCacheStats(0, (base = base + 2));
    this.executor.executePivotQuery(new PivotTableQueryDto(q, List.of(category, ean), List.of()));
    assertCacheStats(0, (base = base + 2));
    this.executor.executePivotQuery(new PivotTableQueryDto(q, List.of(), List.of(category, ean)));
    assertCacheStats(2, base); // same as the previous
    this.executor.executePivotQuery(new PivotTableQueryDto(q, List.of(ean), List.of(category)));
    assertCacheStats(4, base); // Same as the first query but row and category are reversed
    // Same as the first query
    this.executor.executePivotQuery(new PivotTableQueryDto(q, List.of(category), List.of(ean)));
    assertCacheStats(6, base);
  }

  @Test
  void testWithNullValueAndRollup() {
    List<Field> columns = tableFields(List.of("category", "ean"));
    QueryDto q1 = Query
            .from(this.other)
            .select(columns, List.of(CountMeasure.INSTANCE))
            .rollup(columns)
            .build();

    // The first query will put in cache count for the given scope.
    Table result = this.executor.executeQuery(q1);
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, GRAND_TOTAL, 4L),
            List.of("cloth", TOTAL, 1L),
            List.of("cloth", "shirt", 1L),
            List.of("drink", TOTAL, 1L),
            List.of("drink", "bottle", 1L),
            List.of("food", TOTAL, 1L),
            List.of("food", "cookie", 1L),
            Arrays.asList(null, TOTAL, 1L),
            Arrays.asList(null, "other", 1L));
    assertCacheStats(0, 1);

    // The second query has the same scope as the previous, but we do not want to use the cached values for grouping measures.
    QueryDto q2 = Query
            .from(this.other)
            .select(columns, List.of(sum("ca", "price")))
            .rollup(columns)
            .build();

    Table table = this.executor.executeQuery(q2);
    Assertions.assertThat(table).containsExactly(
            List.of(GRAND_TOTAL, GRAND_TOTAL, 20d),
            List.of("cloth", TOTAL, 10d),
            List.of("cloth", "shirt", 10d),
            List.of("drink", TOTAL, 2d),
            List.of("drink", "bottle", 2d),
            List.of("food", TOTAL, 3d),
            List.of("food", "cookie", 3d),
            Arrays.asList(null, TOTAL, 5d),
            Arrays.asList(null, "other", 5d));
    assertCacheStats(1, 2);
  }

  @Test
  void testOrderBy() {
    Measure sum = sum("ps", "price");
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("category")), List.of(sum))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder( // order cannot be guaranteed
            List.of("cloth", 10d),
            List.of("drink", 2d),
            List.of("food", 3d));
    assertCacheStats(0, 2);

    // Execute the same
    query.orderBy(new AliasedField("ps"), OrderKeywordDto.ASC);
    result = this.executor.executeQuery(query);
    assertCacheStats(0, 4);
    Assertions.assertThat(result).containsExactly( // order is always the same
            List.of("drink", 2d),
            List.of("food", 3d),
            List.of("cloth", 10d));

    query.orders.clear();
    query.orderBy(new TableField("category"), OrderKeywordDto.ASC);
    result = this.executor.executeQuery(query);
    assertCacheStats(0, 6);
    Assertions.assertThat(result).containsExactly( // order is always the same
            List.of("cloth", 10d),
            List.of("drink", 2d),
            List.of("food", 3d));
  }

  private void assertCacheStats(int hitCount, int missCount) {
    TestUtil.assertCacheStats(this.queryCache, hitCount, missCount);
  }

  private void assertCacheStats(int hitCount, int missCount, SquashQLUser user) {
    TestUtil.assertCacheStats(this.queryCache, hitCount, missCount, user);
  }

  private static Table execute(QueryExecutor executor, QueryDto query, SquashQLUser user) {
    return executor.executeQuery(
            query,
            CacheStatsDto.builder(),
            user,
            true,
            null,
            createPivotTableContext(query));
  }
}
