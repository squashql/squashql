package io.squashql;

import io.squashql.query.*;
import io.squashql.query.builder.Query;
import io.squashql.query.database.DuckDBQueryEngine;
import io.squashql.query.dto.*;
import io.squashql.query.exception.LimitExceedException;
import io.squashql.query.measure.ParametrizedMeasure;
import io.squashql.table.Table;
import io.squashql.transaction.DuckDBDataLoader;
import io.squashql.type.TableTypedField;
import io.squashql.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.squashql.query.Functions.sum;
import static io.squashql.query.QueryExecutor.createPivotTableContext;
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;
import static io.squashql.query.measure.Repository.VAR;

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
    this.executor.executeQuery(query, CacheStatsDto.builder(), null, true, limitCapture::set, createPivotTableContext(query));
    Assertions.assertThat(limitCapture.get()).isEqualTo(-1);

    int limit = 2;
    query.limit = limit;
    this.executor.executeQuery(query, CacheStatsDto.builder(), null, true, limitCapture::set, createPivotTableContext(query));
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
    TestUtil.assertThatThrownBy(() -> this.executor.executePivotQueryMerge(new PivotTableQueryMergeDto(queryMergeDto, List.of(tableField("eanId")), List.of()), null))
            .isInstanceOf(LimitExceedException.class)
            .hasMessageContaining("too big");
  }

  @Test
  void testOrderBySquashQLMeasure() {
    Runnable loader = () -> this.dl.load(this.storeName, List.of(
            new Object[]{2020, 2d},
            new Object[]{2021, 3d},
            new Object[]{2022, 10d}
    ));
    TableTypedField year = new TableTypedField(this.storeName, "year", int.class);
    TableTypedField price = new TableTypedField(this.storeName, "price", double.class);
    setup(Map.of(this.storeName, List.of(year, price)), loader);

    Measure sum = sum("p", "price");
    Period.Year period = new Period.Year(tableField("year"));
    Measure comp = new ComparisonMeasureReferencePosition(
            "yoy",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            sum,
            Map.of(period.year(), "y-1"),
            period);
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("year")), List.of(comp))
            .orderBy(new AliasedField("yoy"), OrderKeywordDto.DESC)
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of(2022, 7d),
            List.of(2021, 1d),
            Arrays.asList(2020, null));

    query.orders.clear();
    query.orderBy(new AliasedField("yoy"), OrderKeywordDto.ASC);
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of(2021, 1d),
            List.of(2022, 7d),
            Arrays.asList(2020, null)); // null always last
  }

  @Test
  void testOrderBySquashQLMeasureOrderIsPreserved() {
    Runnable loader = () -> {
      List<Object[]> tuples = new ArrayList<>(List.of(
              new Object[]{2020, 0, 2d},
              new Object[]{2020, 1, 3d},
              new Object[]{2021, 0, 3d},
              new Object[]{2021, 1, 5d},
              new Object[]{2022, 0, 10d},
              new Object[]{2022, 1, 5d}
      ));
      Collections.shuffle(tuples);
      this.dl.load(this.storeName, tuples);
    };
    TableTypedField year = new TableTypedField(this.storeName, "year", int.class);
    TableTypedField month = new TableTypedField(this.storeName, "month", int.class);
    TableTypedField price = new TableTypedField(this.storeName, "price", double.class);
    setup(Map.of(this.storeName, List.of(year, month, price)), loader);

    Measure sum = sum("p", "price");
    Period.Year period = new Period.Year(tableField("year"));
    Measure comp = new ComparisonMeasureReferencePosition(
            "yoy",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            sum,
            Map.of(period.year(), "y-1"),
            period);
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("year", "month")), List.of(sum, comp))
            .orderBy(new TableField("year"), OrderKeywordDto.ASC)
            .orderBy(new AliasedField("yoy"), OrderKeywordDto.ASC)
            .build();
    Table result = this.executor.executeQuery(query);
    // Order first by year then by a measure computed by SquashQL. The order should be preserved i.e. year ordered then
    // yoy ordered among year. This is working because orderBy year is applied twice: at DB level and by SquashQL to be
    // able to order "yoy" and preserve the order of year.
    Assertions.assertThat(result).containsExactly(
            Arrays.asList(2020, 0, 2d, null),
            Arrays.asList(2020, 1, 3d, null),
            List.of(2021, 0, 3d, 1d),
            List.of(2021, 1, 5d, 2d),
            List.of(2022, 1, 5d, 0d),
            List.of(2022, 0, 10d, 7d));
  }

  @Test
  void testOrderByWithDuplicates() {
    TableTypedField year = new TableTypedField(this.storeName, "year", int.class);
    TableTypedField ean = new TableTypedField(this.storeName, "ean", String.class);
    TableTypedField category = new TableTypedField(this.storeName, "category", String.class);
    TableTypedField price = new TableTypedField(this.storeName, "price", double.class);
    setup(Map.of(this.storeName, List.of(year, ean, category, price)), () -> this.dl.load(this.storeName, List.of(
            new Object[]{2023, "bottle", "drink", 2d},
            new Object[]{2023, "cookie", "food", 3d},
            new Object[]{2023, "shirt", "cloth", 10d},
            new Object[]{2024, "bottle", "drink", 4d},
            new Object[]{2024, "cookie", "food", 5d},
            new Object[]{2024, "shirt", "cloth", 12d}
    )));

    GroupColumnSetDto group = new GroupColumnSetDto("group", tableField("category"))
            .withNewGroup("Food & Drink", List.of("food", "drink"))
            .withNewGroup("Other", List.of("cloth"));

    // Execute a query where several columns appear multiple times: year and category appears twice.
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("year", "category", "year")), List.of(group), List.of(sum("p", "price")))
            .orderBy(new TableField("year"), OrderKeywordDto.DESC)
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of("Food & Drink", "food", 2024, "food", 2024, 5d),
            List.of("Food & Drink", "drink", 2024, "drink", 2024, 4d),
            List.of("Other", "cloth", 2024, "cloth", 2024, 12d),
            List.of("Food & Drink", "food", 2023, "food", 2023, 3d),
            List.of("Food & Drink", "drink", 2023, "drink", 2023, 2d),
            List.of("Other", "cloth", 2023, "cloth", 2023, 10d));
  }

  @Test
  void testMissingParameterInParametrizedMeasure() {
    TableTypedField date = new TableTypedField(this.storeName, "date", LocalDate.class);
    setup(Map.of(this.storeName, List.of(date)), () -> {
    });
    Measure var = new ParametrizedMeasure(
            "var 95",
            VAR,
            Map.of(
                    // value is missing
                    "date", date,
                    "quantile", 0.95
            ));
    QueryDto query = Query
            .from(this.storeName)
            .select(Collections.emptyList(), List.of(var))
            .build();
    Assertions.assertThatThrownBy(() -> this.executor.executeQuery(query))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Parameter 'value' was expected but not provided");
  }
}
