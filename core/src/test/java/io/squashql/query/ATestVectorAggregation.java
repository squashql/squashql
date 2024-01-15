package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dictionary.ObjectArrayDictionary;
import io.squashql.query.dto.PivotTableQueryDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.ColumnarTable;
import io.squashql.table.PivotTable;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import io.squashql.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.squashql.query.agg.AggregationFunction.SUM;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestVectorAggregation extends ABaseTestQuery {

  static final String productA = "A";
  static final String productB = "B";
  static final String competitorX = "X";
  static final String competitorY = "Y";
  static final String competitorZ = "Z";
  static final LocalDate d1 = LocalDate.of(2023, 1, 1);
  static final LocalDate d2 = LocalDate.of(2023, 1, 2);
  static final LocalDate d3 = LocalDate.of(2023, 1, 3);
  final String storeName = "store" + getClass().getSimpleName().toLowerCase();
  GlobalCache queryCache;

  @Override
  protected void afterSetup() {
    this.queryCache = (GlobalCache) this.executor.queryCache;
  }

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField ean = new TableTypedField(this.storeName, "ean", String.class);
    TableTypedField date = new TableTypedField(this.storeName, "date", LocalDate.class);
    TableTypedField competitor = new TableTypedField(this.storeName, "competitor", String.class);
    TableTypedField price = new TableTypedField(this.storeName, "price", double.class);
    return Map.of(this.storeName, List.of(ean, date, competitor, price));
  }

  @Override
  protected void loadData() {
    this.tm.load(this.storeName, List.of(
            new Object[]{productA, d1, competitorX, 1d},
            new Object[]{productA, d1, competitorY, 2d},
            new Object[]{productA, d1, competitorZ, 3d},
            new Object[]{productA, d2, competitorX, 10d},
            new Object[]{productA, d2, competitorY, 11d},
            new Object[]{productA, d2, competitorZ, 12d},
            new Object[]{productA, d3, competitorX, 100d},
            new Object[]{productA, d3, competitorY, 101d},
            new Object[]{productA, d3, competitorZ, 102d},

            new Object[]{productB, d1, competitorX, 1000d},
            new Object[]{productB, d1, competitorY, 2000d},
            new Object[]{productB, d1, competitorZ, 3000d},
            new Object[]{productB, d2, competitorX, 10000d},
            new Object[]{productB, d2, competitorY, 11000d},
            new Object[]{productB, d2, competitorZ, 12000d},
            new Object[]{productB, d3, competitorX, 100000d},
            new Object[]{productB, d3, competitorY, 101000d},
            new Object[]{productB, d3, competitorZ, 102000d}
    ));
  }

  @Test
  void testCrossjoinOneWithTotals() {
    Field ean = new TableField(this.storeName, "ean");
    Field value = new TableField(this.storeName, "price");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", value, SUM, date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ean), List.of(vector))
            .rollup(List.of(ean))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(ean.name(), vector.alias());
    List<List<Object>> points = List.of(List.of(GRAND_TOTAL), List.of(productA), List.of(productB));
    List<List<Number>> expectedVectors = List.of(
            List.of(6006d, 33033d, 303303d),
            List.of(6d, 33d, 303d),
            List.of(6000d, 33000d, 303000d));
    assertVectorValues((ColumnarTable) result, vector, points, expectedVectors);
  }

  @Test
  void testCrossjoinOneWithoutTotals() {
    Field ean = new TableField(this.storeName, "ean");
    Field value = new TableField(this.storeName, "price");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", value, SUM, date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ean), List.of(vector, CountMeasure.INSTANCE))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(ean.name(), vector.alias(), CountMeasure.ALIAS);
    List<List<Object>> points = List.of(List.of(productA), List.of(productB));
    List<List<Number>> expectedVectors = List.of(
            List.of(6d, 33d, 303d),
            List.of(6000d, 33000d, 303000d));
    assertVectorValues((ColumnarTable) result, vector, points, expectedVectors);
    assertValues((ColumnarTable) result, CountMeasure.INSTANCE, points, List.of(9L, 9L));
  }

  @Test
  void testCrossjoinTwoWithTotals() {
    Field ean = new TableField(this.storeName, "ean");
    Field competitor = new TableField(this.storeName, "competitor");
    Field value = new TableField(this.storeName, "price");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", value, SUM, date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ean, competitor), List.of(vector))
            .rollup(List.of(ean, competitor))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(ean.name(), competitor.name(), vector.alias());
    List<List<Object>> points = List.of(
            List.of(GRAND_TOTAL, GRAND_TOTAL),
            List.of(productA, TOTAL),
            List.of(productA, competitorZ),
            List.of(productA, competitorY),
            List.of(productA, competitorX),
            List.of(productB, TOTAL),
            List.of(productB, competitorZ),
            List.of(productB, competitorY),
            List.of(productB, competitorX));
    List<List<Number>> expectedVectors = List.of(
            List.of(6006d, 33033d, 303303d),
            List.of(6d, 33d, 303d),
            List.of(3.0, 12.0, 102.0),
            List.of(2.0, 11.0, 101.0),
            List.of(1.0, 10.0, 100.0),
            List.of(6000d, 33000d, 303000d),
            List.of(3000.0, 12000.0, 102000.0),
            List.of(2000.0, 11000.0, 101000.0),
            List.of(1000.0, 10000.0, 100000.0));
    assertVectorValues((ColumnarTable) result, vector, points, expectedVectors);
  }

  @Test
  void testCrossjoinTwoWithoutTotals() {
    Field ean = new TableField(this.storeName, "ean");
    Field competitor = new TableField(this.storeName, "competitor");
    Field value = new TableField(this.storeName, "price");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", value, SUM, date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ean, competitor), List.of(vector, CountMeasure.INSTANCE))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(ean.name(), competitor.name(), vector.alias(), CountMeasure.ALIAS);
    List<List<Object>> points = List.of(
            List.of(productA, competitorZ),
            List.of(productA, competitorY),
            List.of(productA, competitorX),
            List.of(productB, competitorZ),
            List.of(productB, competitorY),
            List.of(productB, competitorX));
    List<List<Number>> expectedVectors = List.of(
            List.of(3.0, 12.0, 102.0),
            List.of(2.0, 11.0, 101.0),
            List.of(1.0, 10.0, 100.0),
            List.of(3000.0, 12000.0, 102000.0),
            List.of(2000.0, 11000.0, 101000.0),
            List.of(1000.0, 10000.0, 100000.0));
    assertVectorValues((ColumnarTable) result, vector, points, expectedVectors);
  }

  /**
   * When compute with the vector axis in the query, the aggregation is as it would be without any vectorization.
   */
  @Test
  void testSimpleWithVectorAxisInSelect() {
    Field ean = new TableField(this.storeName, "ean");
    Field value = new TableField(this.storeName, "price");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", value, SUM, date);
    Measure vectorSum = new AggregatedMeasure("vectorSum", value, SUM, false);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ean, date), List.of(vector, vectorSum))
            .rollup(List.of(ean, date))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(ean.name(), date.name(), vector.alias(), vectorSum.alias());
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, GRAND_TOTAL, 342342d, 342342d),
            List.of(productA, TOTAL, 342d, 342d),
            List.of(productA, d1, 6d, 6d),
            List.of(productA, d2, 33d, 33d),
            List.of(productA, d3, 303d, 303d),
            List.of(productB, TOTAL, 342000d, 342000d),
            List.of(productB, d1, 6000d, 6000d),
            List.of(productB, d2, 33000d, 33000d),
            List.of(productB, d3, 303000d, 303000d));
  }

  @Test
  void testSimpleWithOtherMeasure() {
    Field ean = new TableField(this.storeName, "ean");
    Field competitor = new TableField(this.storeName, "competitor");
    Field value = new TableField(this.storeName, "price");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", value, SUM, date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ean, competitor), List.of(vector, CountMeasure.INSTANCE))
            .rollup(List.of(ean, competitor))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(ean.name(), competitor.name(), vector.alias(), CountMeasure.ALIAS);
    List<List<Object>> points = List.of(
            List.of(GRAND_TOTAL, GRAND_TOTAL),
            List.of(productA, TOTAL),
            List.of(productA, competitorZ),
            List.of(productA, competitorY),
            List.of(productA, competitorX),
            List.of(productB, TOTAL),
            List.of(productB, competitorZ),
            List.of(productB, competitorY),
            List.of(productB, competitorX));
    List<List<Number>> expectedVectors = List.of(
            List.of(6006d, 33033d, 303303d),
            List.of(6d, 33d, 303d),
            List.of(3.0, 12.0, 102.0),
            List.of(2.0, 11.0, 101.0),
            List.of(1.0, 10.0, 100.0),
            List.of(6000d, 33000d, 303000d),
            List.of(3000.0, 12000.0, 102000.0),
            List.of(2000.0, 11000.0, 101000.0),
            List.of(1000.0, 10000.0, 100000.0));
    assertVectorValues((ColumnarTable) result, vector, points, expectedVectors);
    List<Number> expectedValues = List.of(18L, 9L, 3L, 3L, 3L, 9L, 3L, 3L, 3L);
    assertValues((ColumnarTable) result, CountMeasure.INSTANCE, points, expectedValues);
  }

  @Test
  void testCache() {
    Field ean = new TableField(this.storeName, "ean");
    Field competitor = new TableField(this.storeName, "competitor");
    Field value = new TableField(this.storeName, "price");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", value, SUM, date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ean, competitor), List.of(vector))
            .rollup(List.of(ean, competitor))
            .build();

    Runnable r = () -> {
      Table result = this.executor.executeQuery(query);
      Assertions.assertThat(result.headers().stream().map(Header::name))
              .containsExactly(ean.name(), competitor.name(), vector.alias());
      List<List<Object>> points = List.of(
              List.of(GRAND_TOTAL, GRAND_TOTAL),
              List.of(productA, TOTAL),
              List.of(productA, competitorZ),
              List.of(productA, competitorY),
              List.of(productA, competitorX),
              List.of(productB, TOTAL),
              List.of(productB, competitorZ),
              List.of(productB, competitorY),
              List.of(productB, competitorX));
      List<List<Number>> expectedVectors = List.of(
              List.of(6006d, 33033d, 303303d),
              List.of(6d, 33d, 303d),
              List.of(3.0, 12.0, 102.0),
              List.of(2.0, 11.0, 101.0),
              List.of(1.0, 10.0, 100.0),
              List.of(6000d, 33000d, 303000d),
              List.of(3000.0, 12000.0, 102000.0),
              List.of(2000.0, 11000.0, 101000.0),
              List.of(1000.0, 10000.0, 100000.0));
      assertVectorValues((ColumnarTable) result, vector, points, expectedVectors);
    };

    int hitCount = (int) this.queryCache.stats(null).hitCount;
    int missCount = (int) this.queryCache.stats(null).missCount;
    r.run();
    TestUtil.assertCacheStats(this.queryCache, hitCount + 0, missCount + 5);

    r.run();
    TestUtil.assertCacheStats(this.queryCache, hitCount + 4, missCount + 5);
  }

  @Test
  void testPivotTable() {
    Field ean = new TableField(this.storeName, "ean");
    Field competitor = new TableField(this.storeName, "competitor");
    Field value = new TableField(this.storeName, "price");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", value, SUM, date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ean, competitor), List.of(vector))
            .build();

    PivotTable result = this.executor.executePivotQuery(new PivotTableQueryDto(query, List.of(competitor), List.of(ean)));
    Assertions.assertThat(result.table.headers().stream().map(Header::name))
            .containsExactly(ean.name(), competitor.name(), vector.alias());
    List<List<Object>> points = List.of(
            List.of(GRAND_TOTAL, GRAND_TOTAL),
            List.of(GRAND_TOTAL, competitorZ),
            List.of(GRAND_TOTAL, competitorY),
            List.of(GRAND_TOTAL, competitorX),
            List.of(productA, GRAND_TOTAL),
            List.of(productA, competitorZ),
            List.of(productA, competitorY),
            List.of(productA, competitorX),
            List.of(productB, GRAND_TOTAL),
            List.of(productB, competitorZ),
            List.of(productB, competitorY),
            List.of(productB, competitorX));
    List<List<Number>> expectedVectors = List.of(
            List.of(6006d, 33033d, 303303d),
            List.of(102102d, 12012d, 3003d),
            List.of(2002d, 11011d, 101101d),
            List.of(100100d, 1001d, 10010d),
            List.of(6d, 33d, 303d),
            List.of(3.0, 12.0, 102.0),
            List.of(2.0, 11.0, 101.0),
            List.of(1.0, 10.0, 100.0),
            List.of(6000d, 33000d, 303000d),
            List.of(3000.0, 12000.0, 102000.0),
            List.of(2000.0, 11000.0, 101000.0),
            List.of(1000.0, 10000.0, 100000.0));
    assertVectorValues((ColumnarTable) result.table, vector, points, expectedVectors);
  }

  private void assertVectorValues(ColumnarTable result, Measure vectorMeasure, List<List<Object>> points, List<List<Number>> expectedVectors) {
    List<Object> aggregateValues = result.getColumnValues(vectorMeasure.alias());
    for (int i = 0; i < points.size(); i++) {
      ObjectArrayDictionary dictionary = result.pointDictionary.get();
      int position = dictionary.getPosition(points.get(i).toArray());
      Object actual = aggregateValues.get(position);
      // SORT to have a deterministic comparison
      List<Number> vector = new ArrayList<>(expectedVectors.get(i)).stream().sorted().toList();
      List<Number> actualVector = new ArrayList<>((List<Number>) actual).stream().sorted().toList();
      Assertions.assertThat(actualVector).containsExactlyElementsOf(vector);
    }
  }

  private void assertValues(ColumnarTable result, Measure otherMeasure, List<List<Object>> points, List<Number> expectedValues) {
    List<Object> aggregateValues = result.getColumnValues(otherMeasure.alias());
    for (int i = 0; i < points.size(); i++) {
      ObjectArrayDictionary dictionary = result.pointDictionary.get();
      int position = dictionary.getPosition(points.get(i).toArray());
      Object actual = aggregateValues.get(position);
      Assertions.assertThat(actual).isEqualTo(expectedValues.get(i));
    }
  }
}
