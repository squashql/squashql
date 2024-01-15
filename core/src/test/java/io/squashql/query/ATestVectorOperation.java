package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.list.Lists;
import io.squashql.query.builder.Query;
import io.squashql.query.compiled.CompiledExpressionMeasure;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import io.squashql.util.MultipleColumnsSorter;
import org.assertj.core.api.Assertions;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;

import static io.squashql.query.ComparisonMethod.DIVIDE;
import static io.squashql.query.agg.AggregationFunction.ANY_VALUE;
import static io.squashql.query.agg.AggregationFunction.SUM;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;
import static io.squashql.util.ListUtils.reorder;
import static java.util.Comparator.naturalOrder;

@TestClass(ignore = TestClass.Type.SNOWFLAKE)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestVectorOperation extends ABaseTestQuery {
  static final String productA = "A";
  static final String productB = "B";
  static final String competitorX = "X";
  static final String competitorY = "Y";
  static final String competitorZ = "Z";
  static final int day = 5;
  static final int month = 4;
  final String storeName = "mystore" + System.currentTimeMillis();// + getClass().getSimpleName().toLowerCase();
  final Field competitor = new TableField(this.storeName, "competitor");
  final Field ean = new TableField(this.storeName, "ean");
  final Field price = new TableField(this.storeName, "price");
  final Field date = new TableField(this.storeName, "date");

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
    // Simulate historical prices
    List<Object[]> l = new ArrayList<>();
    for (String product : List.of(productA, productB)) {
      for (String competitor : List.of(competitorX, competitorY, competitorZ)) {
        for (int d = 1; d < day; d++) {
          for (int m = 1; m < month; m++) {
            l.add(new Object[]{product, LocalDate.of(2023, m, d), competitor, (double) d * m});
          }
        }
      }
    }
    Object[][] array = l.toArray(new Object[0][]);
    this.tm.load(this.storeName, List.of(array));
  }

  @Test
  void testWithoutTotals() {
    Measure vector = new VectorTupleAggMeasure("vector", List.of(Tuples.pair(this.price, SUM), Tuples.pair(this.date, ANY_VALUE)), this.date, null);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(this.competitor, this.ean), List.of(vector))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(this.competitor.name(), this.ean.name(), vector.alias());
    assertVectorTuples(result, vector);
  }

  @Test
  void testTotals() {
    Measure vector = new VectorTupleAggMeasure("vector", List.of(Tuples.pair(this.price, SUM), Tuples.pair(this.date, ANY_VALUE)), this.date, null);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(this.competitor, this.ean), List.of(vector))
            .rollup(List.of(this.competitor, this.ean))
            .build();
    Table result = this.executor.executeQuery(query);

    List<Object> columnValues = result.getColumnValues(vector.alias());
    List<Object> orderedDatesList = new ArrayList<>((int) result.count());
    List<Object> orderedPricesList = new ArrayList<>((int) result.count());
    for (Object columnValue : columnValues) {
      List<Object> tuple = (List<Object>) columnValue;// expected size 2
      Assertions.assertThat(tuple.size()).isEqualTo(2);
      Lists.DoubleList prices = (Lists.DoubleList) tuple.get(0);
      List<LocalDate> dates = (List<LocalDate>) tuple.get(1);
      int[] sort = MultipleColumnsSorter.sort(List.of(dates), List.of(naturalOrder()), new int[0]);
      orderedPricesList.add(reorder(prices, sort));
      orderedDatesList.add(reorder(dates, sort));
    }

    // Create a new table with "fake" measures to be able to check the result
    ColumnarTable orderedTable = new ColumnarTable(
            List.of(result.getHeader(this.competitor.name()), result.getHeader(this.ean.name()), new Header("orderedPrices", Lists.DoubleList.class, true), new Header("orderedDates", List.class, true)),
            Set.of(new CompiledExpressionMeasure("orderedPrices", ""), new CompiledExpressionMeasure("orderedDates", "")),
            List.of(result.getColumnValues(this.competitor.name()), result.getColumnValues(this.ean.name()), orderedPricesList, orderedDatesList));

    Assertions.assertThat(orderedTable.headers().stream().map(Header::name))
            .containsExactly(this.competitor.name(), this.ean.name(), "orderedPrices", "orderedDates");
    List<LocalDate> expectedLocalDates = new ArrayList<>();
    for (int d = 1; d < day; d++) {
      for (int m = 1; m < month; m++) {
        expectedLocalDates.add(LocalDate.of(2023, m, d));
      }
    }
    Collections.sort(expectedLocalDates);
    Assertions.assertThat(orderedTable).containsExactly(
            List.of(GRAND_TOTAL, GRAND_TOTAL, List.of(6.0, 12.0, 18.0, 24.0, 12.0, 24.0, 36.0, 48.0, 18.0, 36.0, 54.0, 72.0), expectedLocalDates),
            List.of(competitorX, TOTAL, List.of(2.0, 4.0, 6.0, 8.0, 4.0, 8.0, 12.0, 16.0, 6.0, 12.0, 18.0, 24.0), expectedLocalDates),
            List.of(competitorX, productA, List.of(1.0, 2.0, 3.0, 4.0, 2.0, 4.0, 6.0, 8.0, 3.0, 6.0, 9.0, 12.0), expectedLocalDates),
            List.of(competitorX, productB, List.of(1.0, 2.0, 3.0, 4.0, 2.0, 4.0, 6.0, 8.0, 3.0, 6.0, 9.0, 12.0), expectedLocalDates),
            List.of(competitorY, TOTAL, List.of(2.0, 4.0, 6.0, 8.0, 4.0, 8.0, 12.0, 16.0, 6.0, 12.0, 18.0, 24.0), expectedLocalDates),
            List.of(competitorY, productA, List.of(1.0, 2.0, 3.0, 4.0, 2.0, 4.0, 6.0, 8.0, 3.0, 6.0, 9.0, 12.0), expectedLocalDates),
            List.of(competitorY, productB, List.of(1.0, 2.0, 3.0, 4.0, 2.0, 4.0, 6.0, 8.0, 3.0, 6.0, 9.0, 12.0), expectedLocalDates),
            List.of(competitorZ, TOTAL, List.of(2.0, 4.0, 6.0, 8.0, 4.0, 8.0, 12.0, 16.0, 6.0, 12.0, 18.0, 24.0), expectedLocalDates),
            List.of(competitorZ, productA, List.of(1.0, 2.0, 3.0, 4.0, 2.0, 4.0, 6.0, 8.0, 3.0, 6.0, 9.0, 12.0), expectedLocalDates),
            List.of(competitorZ, productB, List.of(1.0, 2.0, 3.0, 4.0, 2.0, 4.0, 6.0, 8.0, 3.0, 6.0, 9.0, 12.0), expectedLocalDates));
  }

  @Test
  void testTransformerWithoutTotals() {
    // Dummy transformer. Find an index from the date array and use this index to pick a single price. It is to show
    // we can perform any operation with two vectors
    Function<List<Object>, Object> transformer = (list) -> {
      Lists.DoubleList prices = (Lists.DoubleList) list.get(0);
      List<LocalDate> dates = (List<LocalDate>) list.get(1);
      int index = -1;
      for (int i = 0; i < dates.size(); i++) {
        if (dates.get(i).equals(LocalDate.of(2023, 3, 3))) {
          index = i;
          break;
        }
      }
      return prices.get(index);
    };

    Measure vector = new VectorTupleAggMeasure(
            "price_at_2023_3_3",
            List.of(Tuples.pair(this.price, SUM), Tuples.pair(this.date, ANY_VALUE)),
            this.date,
            transformer);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(this.competitor, this.ean), List.of(vector))
            .rollup(List.of(this.competitor, this.ean))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(this.competitor.name(), this.ean.name(), vector.alias());
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, GRAND_TOTAL, 54d),
            List.of(competitorX, TOTAL, 18d),
            List.of(competitorX, productA, 9d),
            List.of(competitorX, productB, 9d),
            List.of(competitorY, TOTAL, 18d),
            List.of(competitorY, productA, 9d),
            List.of(competitorY, productB, 9d),
            List.of(competitorZ, TOTAL, 18d),
            List.of(competitorZ, productA, 9d),
            List.of(competitorZ, productB, 9d));
  }

  @Test
  void testParentComparison() {
    Measure vector = new VectorTupleAggMeasure("vector", List.of(Tuples.pair(this.price, SUM), Tuples.pair(this.date, ANY_VALUE)), this.date, null);
    List<Field> fields = List.of(this.competitor, this.ean);
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", DIVIDE, vector, fields);

    QueryDto query = Query
            .from(this.storeName)
            .select(fields, List.of(vector, pOp))
            .rollup(fields)
            .build();
    Table result = this.executor.executeQuery(query);
    result.show();
  }

  private void assertVectorTuples(Table result, Measure vector) {
    List<Object> columnValues = result.getColumnValues(vector.alias());
    for (Object columnValue : columnValues) {
      List<Object> tuple = (List<Object>) columnValue;// expected size 2
      Assertions.assertThat(tuple.size()).isEqualTo(2);
      Lists.DoubleList prices = (Lists.DoubleList) tuple.get(0);
      List<LocalDate> dates = (List<LocalDate>) tuple.get(1);
      for (int i = 0; i < dates.size(); i++) {
        Assertions.assertThat(prices.get(i).intValue()).isEqualTo(dates.get(i).getMonth().getValue() * dates.get(i).getDayOfMonth());
      }
    }
  }
}
