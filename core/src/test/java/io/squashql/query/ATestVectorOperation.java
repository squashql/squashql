package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.list.Lists;
import io.squashql.query.builder.Query;
import io.squashql.query.compiled.CompiledExpressionMeasure;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import io.squashql.table.TableUtils;
import io.squashql.type.TableTypedField;
import io.squashql.util.MultipleColumnsSorter;
import org.assertj.core.api.Assertions;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.squashql.query.agg.AggregationFunction.*;
import static java.util.Comparator.naturalOrder;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestVectorOperation extends ABaseTestQuery {
  static final String productA = "A";
  static final String productB = "B";
  static final String competitorX = "X";
  static final String competitorY = "Y";
  static final String competitorZ = "Z";
  final String storeName = "mystore";// + getClass().getSimpleName().toLowerCase();
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
    int day = 5;
//    int day = 29;
    int month = 4;
//    int month = 3;
    // Simulate historical prices
    List<Object[]> l = new ArrayList<>();
    for (String product : List.of(productA, productB)) {
      for (String competitor : List.of(competitorX, competitorY, competitorZ)) {
        for (int d = 1; d < day; d++) {
          for (int m = 1; m < month; m++) {
//            System.out.println(d * m);
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
    Measure vector = new VectorTupleAggMeasure("vector", List.of(Tuples.pair(this.price, SUM), Tuples.pair(this.date, ANY_VALUE)), this.date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(this.competitor, this.ean), List.of(vector))
            .build();
    Table result = this.executor.executeQuery(query);
    result.show();
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(this.competitor.name(), this.ean.name(), vector.alias());
    assertVectorTuples(result, vector);
  }

  @Test
  void testTotals() {
    Measure vector = new VectorTupleAggMeasure("vector", List.of(Tuples.pair(this.price, SUM), Tuples.pair(this.date, ANY_VALUE)), this.date);
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
//      for (int i = 0; i < dates.size(); i++) {
//        Assertions.assertThat(prices.get(i).intValue()).isEqualTo(dates.get(i).getMonth().getValue() * dates.get(i).getDayOfMonth());
//      }
      int[] sort = MultipleColumnsSorter.sort(List.of(dates), List.of(naturalOrder()), new int[0]);
      orderedPricesList.add(TableUtils.reorder(prices, sort));
      orderedDatesList.add(TableUtils.reorder(dates, sort));
    }
    // Create a new table with "fake" measures to be able to check the result
    ColumnarTable lists = new ColumnarTable(
            List.of(result.getHeader(this.competitor.name()), result.getHeader(this.ean.name()), new Header("orderedPrices", Lists.DoubleList.class, true), new Header("orderedDates", List.class, true)),
            Set.of(new CompiledExpressionMeasure("orderedPrices", ""), new CompiledExpressionMeasure("orderedDates", "")),
            List.of(result.getColumnValues(this.competitor.name()), result.getColumnValues(this.ean.name()), orderedPricesList, orderedDatesList));
    result.show();
    lists.show();
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
