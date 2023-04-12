package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.agg.AggregationFunction;
import io.squashql.query.builder.Query;
import io.squashql.query.context.QueryCacheContextValue;
import io.squashql.query.dto.CacheStatsDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.monitoring.QueryWatch;
import io.squashql.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.squashql.query.Functions.*;
import static io.squashql.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static io.squashql.transaction.TransactionManager.SCENARIO_FIELD_NAME;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestQueryCache extends ABaseTestQuery {

  protected String storeName = "store" + getClass().getSimpleName().toLowerCase();
  protected String competitorStoreName = "competitor" + getClass().getSimpleName().toLowerCase();

  protected CaffeineQueryCache queryCache;

  @Override
  protected void afterSetup() {
    this.queryCache = (CaffeineQueryCache) this.executor.queryCache;
  }

  @Override
  protected Map<String, List<Field>> getFieldsByStore() {
    Field ean = new Field(this.storeName, "ean", String.class);
    Field category = new Field(this.storeName, "category", String.class);
    Field price = new Field(this.storeName, "price", double.class);
    Field qty = new Field(this.storeName, "quantity", int.class);

    Field comp_ean = new Field(this.competitorStoreName, "comp_ean", String.class);
    Field comp_name = new Field(this.competitorStoreName, "comp_name", String.class);
    Field comp_price = new Field(this.competitorStoreName, "comp_price", double.class);

    return Map.of(
            this.storeName, List.of(ean, category, price, qty),
            this.competitorStoreName, List.of(comp_ean, comp_name, comp_price)
    );
  }

  @Override
  protected void loadData() {
    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"bottle", "drink", 2d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    this.tm.load(MAIN_SCENARIO_NAME, this.competitorStoreName, List.of(
            new Object[]{"bottle", "A", 2d},
            new Object[]{"bottle", "B", 1d},
            new Object[]{"cookie", "A", 3d},
            new Object[]{"cookie", "B", 2d},
            new Object[]{"shirt", "A", 10d},
            new Object[]{"shirt", "B", 9d}
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
            .select(List.of(SCENARIO_FIELD_NAME), List.of(sum("ps", "price"), sum("qs", "quantity")))
            .build();
    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d, 33l));
    assertCacheStats(0, 3);

    // Execute the same
    result = this.executor.execute(query);
    assertCacheStats(3, 3);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d, 33l));

    // New query but same columns, same measure
    query = Query
            .from(this.storeName)
            .select(List.of(SCENARIO_FIELD_NAME), List.of(sum("qs", "quantity")))
            .build();
    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 33l));
    assertCacheStats(5, 3);
  }

  @Test
  void testQueryDifferentColumns() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(SCENARIO_FIELD_NAME), List.of(sum("ps", "price")))
            .build();
    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d));
    assertCacheStats(0, 2);

    // remove column, no column.
    query = Query
            .from(this.storeName)
            .select(List.of(), List.of(sum("ps", "price")))
            .build();
    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(15.0d));
    assertCacheStats(0, 4);

    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(15.0d));
    assertCacheStats(2, 4);
  }

  @Test
  void testQueryDifferentConditions() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("category"), List.of(sum("ps", "price")))
            .build();
    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("drink", 2d),
            List.of("cloth", 10d),
            List.of("food", 3d));
    assertCacheStats(0, 2);

    query = Query
            .from(this.storeName)
            .where("category", eq("drink"))
            .select(List.of("category"), List.of(sum("ps", "price")))
            .build();
    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("drink", 2d));
    assertCacheStats(0, 4);

    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("drink", 2d));
    assertCacheStats(2, 4);

    query = Query
            .from(this.storeName)
            .where("category", in("food", "cloth"))
            .select(List.of("category"), List.of(sum("ps", "price")))
            .build();
    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("cloth", 10d),
            List.of("food", 3d));
    assertCacheStats(2, 6);
  }

  @Test
  void testQueryWithJoin() {
    QueryDto query = Query
            .from(this.storeName)
            .innerJoin(competitorStoreName)
            .on(this.storeName, "ean", this.competitorStoreName, "comp_ean")
            .select(List.of("category"), List.of(sum("ps", "price")))
            .build();
    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("drink", 4d), // value are doubled because of the join
            List.of("cloth", 20d),
            List.of("food", 6d));
    assertCacheStats(0, 2);

    // Same query but wo. join. Should not hit the cache
    query = Query
            .from(this.storeName)
            .select(List.of("category"), List.of(sum("ps", "price")))
            .build();
    result = this.executor.execute(query);
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
            .select(List.of("category"), List.of(ca)) // ca per category
            .build();

    QueryDto queryDto = Query
            .from(firstSubQuery)
            .select(List.of(), List.of(avg_ca)) // avg of ca
            .build();
    Table result = this.executor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(5d));
    assertCacheStats(0, 2);

    // Change the sub query
    QueryDto secondSubQuery = Query
            .from(this.storeName)
            .select(List.of("category"), List.of(min)) // change agg function
            .build();
    queryDto = Query
            .from(secondSubQuery)
            .select(List.of(), List.of(avg_ca)) // avg of ca
            .build();
    result = this.executor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(5d));
    assertCacheStats(0, 4);

    // Change again the sub query
    secondSubQuery = Query
            .from(this.storeName)
            .select(List.of("ean"), List.of(min)) // change here
            .build();
    queryDto = Query
            .from(secondSubQuery)
            .select(List.of(), List.of(avg_ca)) // avg of ca
            .build();
    result = this.executor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(5d));
    assertCacheStats(0, 6);

    // Hit the cache
    queryDto = Query
            .from(firstSubQuery) // same first sub-query
            .select(List.of(), List.of(avg_ca, sum_ca)) // ask for another measure
            .build();
    result = this.executor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(5d, 15d));
    assertCacheStats(2, 7); // avg_ca and count hit the cache
  }

  @Test
  void testQueryWithSubQueryAndColumnsAndConditions() {
    QueryDto firstSubQuery = Query
            .from(this.storeName)
            .select(List.of("category", "ean"), List.of(sum("ca", "price"))) // ca per scenario
            .build();

    QueryDto queryDto = Query
            .from(firstSubQuery)
            .select(List.of("ean"), List.of(avg("mean", "ca"))) // ean needs to be in the subquery to make it work!
            .build();
    Table result = this.executor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(
            List.of("bottle", 2d),
            List.of("cookie", 3d),
            List.of("shirt", 10d));
  }

  @Test
  void testUseCacheContextValue() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(), List.of(sum("ps", "price")))
            .build();
    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(15d));
    assertCacheStats(0, 2);

    // Execute the same
    result = this.executor.execute(query.context(QueryCacheContextValue.KEY, new QueryCacheContextValue(QueryCacheContextValue.Action.NOT_USE)));
    // No cache so no hitCount, no missCount changes
    assertCacheStats(0, 2);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(15d));

    query.context(QueryCacheContextValue.KEY, new QueryCacheContextValue(QueryCacheContextValue.Action.USE));
    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(15d));
    assertCacheStats(2, 2);

    query.context(QueryCacheContextValue.KEY, new QueryCacheContextValue(QueryCacheContextValue.Action.INVALIDATE));
    // Invalidate should empty the cache and fill it with new values.
    result = this.executor.execute(query);
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
    QueryExecutor executor = new QueryExecutor(this.createQueryEngine(this.datastore), cache);

    Supplier<QueryDto> querySupplier = () -> new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("ps", "price", AggregationFunction.SUM);
    // Scope 1 added to the cache
    executor.execute(querySupplier.get());
    assertCacheStats(cache.stats(), 0, 2);

    // Scope 2 added to the cache
    executor.execute(querySupplier.get().withColumn("category"));
    assertCacheStats(cache.stats(), 0, 4);

    // Scope 3, should evict an entry in the cache
    executor.execute(querySupplier.get().withCondition("category", Functions.eq("drink")));
    latch.await(60, TimeUnit.SECONDS);
    CacheStatsDto stats = cache.stats();
    assertCacheStats(stats, 0, 6);
    Assertions.assertThat(c.getAndIncrement()).isEqualTo(1);
    Assertions.assertThat(stats.evictionCount).isEqualTo(1);
  }

  @Test
  void testQueryDifferentUsers() {
    BasicUser paul = new BasicUser("paul");
    BasicUser peter = new BasicUser("peter");

    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(SCENARIO_FIELD_NAME), List.of(sum("ps", "price"), sum("qs", "quantity")))
            .build();
    Table result = execute(this.executor, query, paul);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d, 33l));
    assertCacheStats(0, 3);

    // Execute the same query, same user
    result = execute(this.executor, query, paul);
    assertCacheStats(3, 3);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d, 33l));

    // Execute the same query, but different user
    result = execute(this.executor, query, peter);
    assertCacheStats(3, 6);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d, 33l));
  }

  @Test
  void testQueryDifferentUsersWithSubQuery() {
    BasicUser paul = new BasicUser("paul");
    BasicUser peter = new BasicUser("peter");

    Measure ca = sum("ca", "price");
    Measure avg_ca = avg("mean_ca", "ca");

    QueryDto subQuery = Query
            .from(this.storeName)
            .select(List.of("category"), List.of(ca)) // ca per category
            .build();

    QueryDto queryDto = Query
            .from(subQuery)
            .select(List.of(), List.of(avg_ca)) // avg of ca
            .build();
    Table result = execute(this.executor, queryDto, paul);
    Assertions.assertThat(result).containsExactly(List.of(5d));
    assertCacheStats(0, 2);

    // Same query, same user
    execute(this.executor, queryDto, paul);
    assertCacheStats(2, 2);

    // Same query, different user
    execute(this.executor, queryDto, peter);
    assertCacheStats(2, 4);
  }

  @Test
  void testQueryDifferentLimits() {
    // No limit, it will use the default value (10k)
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("ean"), List.of(sum("ps", "price")))
            .build();
    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("bottle", 2d),
            List.of("cookie", 3d),
            List.of("shirt", 10d));
    assertCacheStats(0, 2);

    int limit = 2;
    query.withLimit(limit);
    result = this.executor.execute(query);
    assertCacheStats(0, 4);
    Assertions.assertThat(result.count()).isEqualTo(limit);

    // Same with same limit
    result = this.executor.execute(query);
    assertCacheStats(2, 4);
    Assertions.assertThat(result.count()).isEqualTo(limit);

    // Same with different limit
    int otherLimit = 1;
    query.withLimit(otherLimit);
    result = this.executor.execute(query);
    assertCacheStats(2, 6);
    Assertions.assertThat(result.count()).isEqualTo(otherLimit);
  }

  @Test
  void testCacheWithFullPath() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(this.storeName + "." + SCENARIO_FIELD_NAME), List.of())
            .build();
    this.executor.execute(query);
    assertCacheStats(0, 1);

    query = Query
            .from(this.storeName)
            .select(List.of(this.storeName + "." + SCENARIO_FIELD_NAME), List.of())
            .build();
    this.executor.execute(query);
    assertCacheStats(1, 1);
  }

  private void assertCacheStats(int hitCount, int missCount) {
    CacheStatsDto stats = this.queryCache.stats();
    assertCacheStats(stats, hitCount, missCount);
  }

  private void assertCacheStats(CacheStatsDto stats, int hitCount, int missCount) {
    Assertions.assertThat(stats.hitCount).isEqualTo(hitCount);
    Assertions.assertThat(stats.missCount).isEqualTo(missCount);
  }

  private static Table execute(QueryExecutor executor, QueryDto query, SquashQLUser user) {
    return executor.execute(
            query,
            new QueryWatch(),
            CacheStatsDto.builder(),
            user,
            true);
  }
}
