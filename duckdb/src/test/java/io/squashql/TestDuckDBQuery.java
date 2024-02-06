package io.squashql;

import io.squashql.query.QueryExecutor;
import io.squashql.query.builder.Query;
import io.squashql.query.database.DuckDBQueryEngine;
import io.squashql.query.dto.*;
import io.squashql.query.exception.LimitExceedException;
import io.squashql.transaction.DuckDBDataLoader;
import io.squashql.type.TableTypedField;
import io.squashql.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.squashql.query.Functions.sum;
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;

public class TestDuckDBQuery {
  protected String storeName = "myStore";
  protected DuckDBDatastore datastore;
  protected DuckDBQueryEngine queryEngine;
  protected DuckDBDataLoader dl;
  protected QueryExecutor executor;

  void setup(Map<String, List<TableTypedField>> fieldsByStore, Runnable dataLoading) {
    this.datastore = new DuckDBDatastore();
    this.dl = new DuckDBDataLoader(this.datastore);
    fieldsByStore.forEach(this.dl::createOrReplaceTable);
    this.queryEngine = new DuckDBQueryEngine(this.datastore);
    this.executor = new QueryExecutor(this.queryEngine);
    dataLoading.run();
  }

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

  protected void loadData() {
    this.dl.load(this.storeName, List.of(
            new Object[]{0, "bottle", "drink", null, 2d, 10, true},
            new Object[]{1, "cookie", "food", "biscuit", 3d, 20, true},
            new Object[]{2, "shirt", "cloth", null, 10d, 3, false}
    ));
  }

  @Test
  void testQueryLimitNotifier() {
    setup(getFieldsByStore(), this::loadData);

    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("eanId")), List.of(sum("p", "price"), sum("q", "quantity")))
            .build();

    AtomicInteger limitCapture = new AtomicInteger(-1);
    this.executor.executeQuery(query, CacheStatsDto.builder(), null, true, limitCapture::set);
    Assertions.assertThat(limitCapture.get()).isEqualTo(-1);

    int limit = 2;
    query.limit = limit;
    this.executor.executeQuery(query, CacheStatsDto.builder(), null, true, limitCapture::set);
    Assertions.assertThat(limitCapture.get()).isEqualTo(limit);
  }

  @Test
  void testMergeTablesAboveQueryLimit() {
    setup(getFieldsByStore(), this::loadData);

    QueryDto query1 = Query
            .from(this.storeName)
            .select(tableFields(List.of("eanId")), List.of(sum("p", "price")))
            .build();

    QueryDto query2 = Query
            .from(this.storeName)
            .select(tableFields(List.of("eanId")), List.of(sum("q", "quantity")))
            .build();

    query1.limit = 2;
    QueryMergeDto queryMergeDto = QueryMergeDto.from(query1).join(query2, JoinType.INNER);
    TestUtil.assertThatThrownBy(() -> this.executor.executeQueryMerge(queryMergeDto, null))
            .isInstanceOf(LimitExceedException.class)
            .hasMessageContaining("too big");
    TestUtil.assertThatThrownBy(() -> this.executor.executePivotQueryMerge(new PivotTableQueryMergeDto(queryMergeDto, List.of(tableField("eanId")), List.of(), false), null))
            .isInstanceOf(LimitExceedException.class)
            .hasMessageContaining("too big");
  }
}
