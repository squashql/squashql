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

  static final String mmm = "MMM";
  static final String vblax = "VBLAX";
  static final String volatilityReturn = "VolatilityReturn";
  static final String fxReturn = "FXReturn";
  static final String equityReturn = "EquityReturn";
  static final LocalDate d1 = LocalDate.of(2023, 1, 1);
  static final LocalDate d2 = LocalDate.of(2023, 1, 2);
  static final LocalDate d3 = LocalDate.of(2023, 1, 3);
  String storeName = "store" + getClass().getSimpleName().toLowerCase();
  GlobalCache queryCache;

  @Override
  protected void afterSetup() {
    this.queryCache = (GlobalCache) this.executor.queryCache;
  }

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField ticker = new TableTypedField(this.storeName, "ticker", String.class);
    TableTypedField date = new TableTypedField(this.storeName, "date", LocalDate.class);
    TableTypedField riskType = new TableTypedField(this.storeName, "riskType", String.class);
    TableTypedField value = new TableTypedField(this.storeName, "value", double.class);
    TableTypedField valueInt = new TableTypedField(this.storeName, "valueInt", int.class); // same but type is different
    return Map.of(this.storeName, List.of(ticker, date, riskType, value, valueInt));
  }

  @Override
  protected void loadData() {
    this.tm.load(this.storeName, List.of(
            new Object[]{mmm, d1, volatilityReturn, 1d, 1},
            new Object[]{mmm, d1, fxReturn, 2d, 2},
            new Object[]{mmm, d1, equityReturn, 3d, 3},
            new Object[]{mmm, d2, volatilityReturn, 10d, 10},
            new Object[]{mmm, d2, fxReturn, 11d, 11},
            new Object[]{mmm, d2, equityReturn, 12d, 12},
            new Object[]{mmm, d3, volatilityReturn, 100d, 100},
            new Object[]{mmm, d3, fxReturn, 101d, 101},
            new Object[]{mmm, d3, equityReturn, 102d, 102},

            new Object[]{vblax, d1, volatilityReturn, 1000d, 1000},
            new Object[]{vblax, d1, fxReturn, 2000d, 2000},
            new Object[]{vblax, d1, equityReturn, 3000d, 3000},
            new Object[]{vblax, d2, volatilityReturn, 10000d, 10000},
            new Object[]{vblax, d2, fxReturn, 11000d, 11000},
            new Object[]{vblax, d2, equityReturn, 12000d, 12000},
            new Object[]{vblax, d3, volatilityReturn, 100000d, 100000},
            new Object[]{vblax, d3, fxReturn, 101000d, 101000},
            new Object[]{vblax, d3, equityReturn, 102000d, 102000}
    ));
  }

  @Test
  void testCrossjoinOneWithTotals() {
    Field ticker = new TableField(this.storeName, "ticker");
    Field value = new TableField(this.storeName, "value");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", value, SUM, date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ticker), List.of(vector))
            .rollup(List.of(ticker))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(ticker.name(), vector.alias());
    List<List<Object>> points = List.of(List.of(GRAND_TOTAL), List.of(mmm), List.of(vblax));
    List<List<Number>> expectedVectors = List.of(
            List.of(6006d, 33033d, 303303d),
            List.of(6d, 33d, 303d),
            List.of(6000d, 33000d, 303000d));
    assertVectorValues((ColumnarTable) result, vector, points, expectedVectors);
  }

  @Test
  void testCrossjoinOneWithoutTotals() {
    Field ticker = new TableField(this.storeName, "ticker");
    Field value = new TableField(this.storeName, "value");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", value, SUM, date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ticker), List.of(vector, CountMeasure.INSTANCE))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(ticker.name(), vector.alias(), CountMeasure.ALIAS);
    List<List<Object>> points = List.of(List.of(mmm), List.of(vblax));
    List<List<Number>> expectedVectors = List.of(
            List.of(6d, 33d, 303d),
            List.of(6000d, 33000d, 303000d));
    assertVectorValues((ColumnarTable) result, vector, points, expectedVectors);
    assertValues((ColumnarTable) result, CountMeasure.INSTANCE, points, List.of(9L, 9L));
  }

  @Test
  void testCrossjoinTwoWithTotals() {
    Field ticker = new TableField(this.storeName, "ticker");
    Field riskType = new TableField(this.storeName, "riskType");
    Field value = new TableField(this.storeName, "value");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", value, SUM, date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ticker, riskType), List.of(vector))
            .rollup(List.of(ticker, riskType))
            .build();
    Table result = this.executor.executeQuery(query);
    result.show();
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(ticker.name(), riskType.name(), vector.alias());
    List<List<Object>> points = List.of(
            List.of(GRAND_TOTAL, GRAND_TOTAL),
            List.of(mmm, TOTAL),
            List.of(mmm, equityReturn),
            List.of(mmm, fxReturn),
            List.of(mmm, volatilityReturn),
            List.of(vblax, TOTAL),
            List.of(vblax, equityReturn),
            List.of(vblax, fxReturn),
            List.of(vblax, volatilityReturn));
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
    Field ticker = new TableField(this.storeName, "ticker");
    Field riskType = new TableField(this.storeName, "riskType");
    Field value = new TableField(this.storeName, "value");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", value, SUM, date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ticker, riskType), List.of(vector, CountMeasure.INSTANCE))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(ticker.name(), riskType.name(), vector.alias(), CountMeasure.ALIAS);
    List<List<Object>> points = List.of(
            List.of(mmm, equityReturn),
            List.of(mmm, fxReturn),
            List.of(mmm, volatilityReturn),
            List.of(vblax, equityReturn),
            List.of(vblax, fxReturn),
            List.of(vblax, volatilityReturn));
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
    Field ticker = new TableField(this.storeName, "ticker");
    Field value = new TableField(this.storeName, "value");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", value, SUM, date);
    Measure vectorSum = new AggregatedMeasure("vectorSum", value, SUM, false);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ticker, date), List.of(vector, vectorSum))
            .rollup(List.of(ticker, date))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(ticker.name(), date.name(), vector.alias(), vectorSum.alias());
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, GRAND_TOTAL, 342342d, 342342d),
            List.of(mmm, TOTAL, 342d, 342d),
            List.of(mmm, d1, 6d, 6d),
            List.of(mmm, d2, 33d, 33d),
            List.of(mmm, d3, 303d, 303d),
            List.of(vblax, TOTAL, 342000d, 342000d),
            List.of(vblax, d1, 6000d, 6000d),
            List.of(vblax, d2, 33000d, 33000d),
            List.of(vblax, d3, 303000d, 303000d));
  }

  @Test
  void testSimpleWithOtherMeasure() {
    Field ticker = new TableField(this.storeName, "ticker");
    Field riskType = new TableField(this.storeName, "riskType");
    Field value = new TableField(this.storeName, "value");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", value, SUM, date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ticker, riskType), List.of(vector, CountMeasure.INSTANCE))
            .rollup(List.of(ticker, riskType))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(ticker.name(), riskType.name(), vector.alias(), CountMeasure.ALIAS);
    List<List<Object>> points = List.of(
            List.of(GRAND_TOTAL, GRAND_TOTAL),
            List.of(mmm, TOTAL),
            List.of(mmm, equityReturn),
            List.of(mmm, fxReturn),
            List.of(mmm, volatilityReturn),
            List.of(vblax, TOTAL),
            List.of(vblax, equityReturn),
            List.of(vblax, fxReturn),
            List.of(vblax, volatilityReturn));
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
    Field ticker = new TableField(this.storeName, "ticker");
    Field riskType = new TableField(this.storeName, "riskType");
    Field value = new TableField(this.storeName, "value");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", value, SUM, date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ticker, riskType), List.of(vector))
            .rollup(List.of(ticker, riskType))
            .build();

    Runnable r = () -> {
      Table result = this.executor.executeQuery(query);
      Assertions.assertThat(result.headers().stream().map(Header::name))
              .containsExactly(ticker.name(), riskType.name(), vector.alias());
      List<List<Object>> points = List.of(
              List.of(GRAND_TOTAL, GRAND_TOTAL),
              List.of(mmm, TOTAL),
              List.of(mmm, equityReturn),
              List.of(mmm, fxReturn),
              List.of(mmm, volatilityReturn),
              List.of(vblax, TOTAL),
              List.of(vblax, equityReturn),
              List.of(vblax, fxReturn),
              List.of(vblax, volatilityReturn));
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
    TestUtil.assertCacheStats(this.queryCache, hitCount + 0, missCount + 7);

    r.run();
    TestUtil.assertCacheStats(this.queryCache, hitCount + 6, missCount + 7);
  }

  @Test
  void testPivotTable() {
    Field ticker = new TableField(this.storeName, "ticker");
    Field riskType = new TableField(this.storeName, "riskType");
    Field value = new TableField(this.storeName, "value");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vector", value, SUM, date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(ticker, riskType), List.of(vector))
            .build();

    PivotTable result = this.executor.executePivotQuery(new PivotTableQueryDto(query, List.of(riskType), List.of(ticker)));
    Assertions.assertThat(result.table.headers().stream().map(Header::name))
            .containsExactly(ticker.name(), riskType.name(), vector.alias());
    List<List<Object>> points = List.of(
            List.of(GRAND_TOTAL, GRAND_TOTAL),
            List.of(GRAND_TOTAL, equityReturn),
            List.of(GRAND_TOTAL, fxReturn),
            List.of(GRAND_TOTAL, volatilityReturn),
            List.of(mmm, GRAND_TOTAL),
            List.of(mmm, equityReturn),
            List.of(mmm, fxReturn),
            List.of(mmm, volatilityReturn),
            List.of(vblax, GRAND_TOTAL),
            List.of(vblax, equityReturn),
            List.of(vblax, fxReturn),
            List.of(vblax, volatilityReturn));
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