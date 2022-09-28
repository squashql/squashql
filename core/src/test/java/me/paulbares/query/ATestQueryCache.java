package me.paulbares.query;

import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.context.QueryCacheContextValue;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.CacheStatsDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.TableDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static me.paulbares.query.QueryBuilder.*;
import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static me.paulbares.transaction.TransactionManager.SCENARIO_FIELD_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestQueryCache {

  protected Datastore datastore;

  protected QueryExecutor queryExecutor;
  protected CaffeineQueryCache queryCache;

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

    Field comp_ean = new Field("comp_ean", String.class);
    Field comp_name = new Field("comp_name", String.class);
    Field comp_price = new Field("comp_price", double.class);

    this.datastore = createDatastore();
    QueryEngine queryEngine = createQueryEngine(this.datastore);
    this.queryExecutor = new QueryExecutor(queryEngine);
    this.queryCache = (CaffeineQueryCache) this.queryExecutor.queryCache;
    this.tm = createTransactionManager();

    beforeLoad(List.of(ean, category, price, qty), List.of(comp_ean, comp_name, comp_price));

    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"bottle", "drink", 2d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    this.tm.load(MAIN_SCENARIO_NAME, "competitor", List.of(
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

  protected void beforeLoad(List<Field> baseStoreFields, List<Field> targetStoreFields) {
  }

  @Test
  void testQuerySameColumns() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("ps", "price", AggregationFunction.SUM)
            .aggregatedMeasure("qs", "quantity", AggregationFunction.SUM);
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d, 33l));
    assertCacheStats(0, 3);

    // Execute the same
    result = this.queryExecutor.execute(query);
    assertCacheStats(3, 3);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d, 33l));

    // New query but same columns, same measure
    query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("qs", "quantity", AggregationFunction.SUM);
    result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 33l));
    assertCacheStats(5, 3);
  }

  @Test
  void testQueryDifferentColumns() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("ps", "price", AggregationFunction.SUM);
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d));
    assertCacheStats(0, 2);

    // remove column, no column.
    query = new QueryDto()
            .table(this.storeName)
            .aggregatedMeasure("ps", "price", AggregationFunction.SUM);
    result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(15.0d));
    assertCacheStats(0, 4);

    result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(15.0d));
    assertCacheStats(2, 4);
  }

  @Test
  void testQueryDifferentConditions() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn("category")
            .aggregatedMeasure("ps", "price", AggregationFunction.SUM);
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("drink", 2d),
            List.of("cloth", 10d),
            List.of("food", 3d));
    assertCacheStats(0, 2);

    query.withCondition("category", QueryBuilder.eq("drink"));
    result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("drink", 2d));
    assertCacheStats(0, 4);

    query.withCondition("category", QueryBuilder.eq("drink"));
    result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("drink", 2d));
    assertCacheStats(2, 4);

    query.withCondition("category", QueryBuilder.in("food", "cloth"));
    result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("cloth", 10d),
            List.of("food", 3d));
    assertCacheStats(2, 6);
  }

  @Test
  void testQueryWithJoin() {
    TableDto table = QueryBuilder.table(this.storeName);
    table.innerJoin(QueryBuilder.table("competitor"), "ean", "comp_ean");

    QueryDto query = new QueryDto()
            .table(table)
            .withColumn("category")
            .aggregatedMeasure("ps", "price", AggregationFunction.SUM);
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("drink", 4d), // value are doubled because of the join
            List.of("cloth", 20d),
            List.of("food", 6d));
    assertCacheStats(0, 2);

    // Same query but wo. join. Should not hit the cache
    query = new QueryDto()
            .table(this.storeName)
            .withColumn("category")
            .aggregatedMeasure("ps", "price", AggregationFunction.SUM);
    result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("drink", 2d),
            List.of("cloth", 10d),
            List.of("food", 3d));
    assertCacheStats(0, 4);
  }

  @Test
  void testWithSubQuery() {
    QueryDto firstSubQuery = new QueryDto()
            .table(this.storeName)
            .withColumn("category")
            .withMeasure(sum("ca", "price")); // ca per scenario

    QueryDto queryDto = new QueryDto()
            .table(firstSubQuery)
            .withMeasure(avg("mean", "ca"));// avg of ca
    Table result = this.queryExecutor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(5d));
    assertCacheStats(0, 2);

    // Change the sub query
    QueryDto subQuery = new QueryDto()
            .table(this.storeName)
            .withColumn("category")
            .withMeasure(min("ca", "price")); // change agg function
    queryDto = new QueryDto()
            .table(subQuery)
            .withMeasure(avg("mean", "ca"));// avg of ca
    result = this.queryExecutor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(5d));
    assertCacheStats(0, 4);

    // Change again the sub query
    subQuery = new QueryDto()
            .table(this.storeName)
            .withColumn("ean") // change here
            .withMeasure(min("ca", "price"));
    queryDto = new QueryDto()
            .table(subQuery)
            .withMeasure(avg("mean", "ca"));// avg of ca
    result = this.queryExecutor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(5d));
    assertCacheStats(0, 6);

    // Hit the cache
    queryDto = new QueryDto()
            .table(firstSubQuery) // same first sub-query
            .withMeasure(avg("mean", "ca"))
            .withMeasure(sum("mean", "ca"));// ask for another measure
    result = this.queryExecutor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(5d, 15d));
    assertCacheStats(1, 7);
  }

  @Test
  void testQueryWithSubQueryAndColumnsAndConditions() {
    QueryDto firstSubQuery = new QueryDto()
            .table(this.storeName)
            .withColumn("category")
            .withColumn("ean")
            .withMeasure(sum("ca", "price")); // ca per scenario

    QueryDto queryDto = new QueryDto()
            .table(firstSubQuery)
            .withColumn("ean") // ean needs to be in the subquery to make it work!
            .withMeasure(avg("mean", "ca"));// avg of ca
    Table result = this.queryExecutor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(
            List.of("bottle", 2d),
            List.of("cookie", 3d),
            List.of("shirt", 10d));
  }

  @Test
  void testUseCacheContextValue() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .aggregatedMeasure("ps", "price", AggregationFunction.SUM);
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(15d));
    assertCacheStats(0, 2);

    // Execute the same
    result = this.queryExecutor.execute(query.context(QueryCacheContextValue.KEY, new QueryCacheContextValue(QueryCacheContextValue.Action.NOT_USE)));
    // No cache so no hitCount, no missCount changes
    assertCacheStats(0, 2);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(15d));

    query.context(QueryCacheContextValue.KEY, new QueryCacheContextValue(QueryCacheContextValue.Action.USE));
    result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of(15d));
    assertCacheStats(2, 2);

    query.context(QueryCacheContextValue.KEY, new QueryCacheContextValue(QueryCacheContextValue.Action.INVALIDATE));
    // Invalidate should empty the cache and fill it with new values.
    result = this.queryExecutor.execute(query);
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
    executor.execute(querySupplier.get().withCondition("category", QueryBuilder.eq("drink")));
    latch.await(60, TimeUnit.SECONDS);
    CacheStatsDto stats = cache.stats();
    assertCacheStats(stats, 0, 6);
    Assertions.assertThat(c.getAndIncrement()).isEqualTo(1);
    Assertions.assertThat(stats.evictionCount).isEqualTo(1);
  }

  private void assertCacheStats(int hitCount, int missCount) {
    CacheStatsDto stats = this.queryCache.stats();
    assertCacheStats(stats, hitCount, missCount);
  }

  private void assertCacheStats(CacheStatsDto stats, int hitCount, int missCount) {
    Assertions.assertThat(stats.hitCount).isEqualTo(hitCount);
    Assertions.assertThat(stats.missCount).isEqualTo(missCount);
  }
}
