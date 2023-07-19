package io.squashql;

import io.squashql.query.QueryExecutor;
import io.squashql.query.builder.Query;
import io.squashql.query.database.DuckDBQueryEngine;
import io.squashql.query.dto.CacheStatsDto;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.TypedField;
import io.squashql.transaction.DuckDBDataLoader;
import io.squashql.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.squashql.query.Functions.sum;

public class TestDuckDBQuery {
  protected String storeName = "myStore";
  protected DuckDBDatastore datastore;
  protected DuckDBQueryEngine queryEngine;
  protected DuckDBDataLoader dl;
  protected QueryExecutor executor;

  void setup(Map<String, List<TypedField>> fieldsByStore, Runnable dataLoading) {
    this.datastore = new DuckDBDatastore();
    this.dl = new DuckDBDataLoader(this.datastore);
    fieldsByStore.forEach(this.dl::createOrReplaceTable);
    this.queryEngine = new DuckDBQueryEngine(this.datastore);
    this.executor = new QueryExecutor(this.queryEngine);
    dataLoading.run();
  }

  protected Map<String, List<TypedField>> getFieldsByStore() {
    TypedField ean = new TypedField(this.storeName, "ean", String.class);
    TypedField eanId = new TypedField(this.storeName, "eanId", int.class);
    TypedField category = new TypedField(this.storeName, "category", String.class);
    TypedField subcategory = new TypedField(this.storeName, "subcategory", String.class);
    TypedField price = new TypedField(this.storeName, "price", double.class);
    TypedField qty = new TypedField(this.storeName, "quantity", int.class);
    TypedField isFood = new TypedField(this.storeName, "isFood", boolean.class);
    return Map.of(this.storeName, List.of(eanId, ean, category, subcategory, price, qty, isFood));
  }

  protected void loadData() {
    this.dl.load(this.storeName, List.of(
            new Object[]{0, "bottle", "drink", null, 2d, 10, true},
            new Object[]{1, "cookie", "food", "biscuit", 3d, 20, true},
            new Object[]{2, "shirt", "cloth", null, 10d, 3, false}
    ));
  }

  @Test
  void testQueryLimitNotifier() {
    setup(getFieldsByStore(), () -> loadData());

    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("eanId"), List.of(sum("p", "price"), sum("q", "quantity")))
            .build();

    AtomicInteger limitCapture = new AtomicInteger(-1);
    this.executor.execute(query, null, CacheStatsDto.builder(), null, true, limitCapture::set);
    Assertions.assertThat(limitCapture.get()).isEqualTo(-1);

    int limit = 2;
    query.limit = limit;
    this.executor.execute(query, null, CacheStatsDto.builder(), null, true, limitCapture::set);
    Assertions.assertThat(limitCapture.get()).isEqualTo(limit);
  }

  @Test
  void testMergeTablesAboveQueryLimit() {
    setup(getFieldsByStore(), () -> loadData());

    QueryDto query1 = Query
            .from(this.storeName)
            .select(List.of("eanId"), List.of(sum("p", "price")))
            .build();

    QueryDto query2 = Query
            .from(this.storeName)
            .select(List.of("eanId"), List.of(sum("q", "quantity")))
            .build();

    query1.limit = 2;
    TestUtil.assertThatThrownBy(() -> this.executor.execute(query1, query2, JoinType.INNER, null))
            .hasMessageContaining("too big");
  }
}
