package me.paulbares.query;

import com.google.common.cache.CacheStats;
import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.database.QueryEngine;
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

import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static me.paulbares.transaction.TransactionManager.SCENARIO_FIELD_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestQueryCache {

  protected Datastore datastore;

  protected QueryExecutor queryExecutor;
  protected QueryCache queryCache;

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
    this.queryCache = this.queryExecutor.queryCache;
    this.tm = createTransactionManager();

    beforeLoading(List.of(ean, category, price, qty), List.of(comp_ean, comp_name, comp_price));

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

  protected void beforeLoading(List<Field> baseStoreFields, List<Field> targetStoreFields) {
  }

  @Test
  void testQuerySameColumns() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("price", AggregationFunction.SUM)
            .aggregatedMeasure("quantity", AggregationFunction.SUM);
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d, 33l));
    assertCacheStats(0, 3);

    // Execute the same
    this.queryExecutor.execute(query);
    assertCacheStats(3, 3);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d, 33l));

    // New query but same columns, same measure
    query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("quantity", AggregationFunction.SUM);
    result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 33l));
    assertCacheStats(5, 3);
  }

  @Test
  void testQueryDifferentColumns() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .withColumn(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("price", AggregationFunction.SUM);
    Table result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(List.of("base", 15.0d));
    assertCacheStats(0, 2);

    // remove column, no column.
    query = new QueryDto()
            .table(this.storeName)
            .aggregatedMeasure("price", AggregationFunction.SUM);
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
            .aggregatedMeasure("price", AggregationFunction.SUM);
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
            .aggregatedMeasure("price", AggregationFunction.SUM);
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
            .aggregatedMeasure("price", AggregationFunction.SUM);
    result = this.queryExecutor.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("drink", 2d),
            List.of("cloth", 10d),
            List.of("food", 3d));
    assertCacheStats(0, 4);
  }

  private void assertCacheStats(int hitCount, int missCount) {
    CacheStats stats = this.queryCache.stats();
    Assertions.assertThat(stats.hitCount()).isEqualTo(hitCount);
    Assertions.assertThat(stats.missCount()).isEqualTo(missCount);
  }
}
