package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.squashql.query.agg.AggregationFunction.AVG;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestVectorOperation extends ABaseTestQuery {

  static final String productA = "A";
  static final String productB = "B";
  static final String competitorX = "X";
  static final String competitorY = "Y";
  static final String competitorZ = "Z";
  String storeName = "mystore";// + getClass().getSimpleName().toLowerCase();

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
    int day = 29;
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
  void test() {
    this.executor.executeRaw("select * from " + this.storeName)
            .show();
    Field competitor = new TableField(this.storeName, "competitor");
    Field ean = new TableField(this.storeName, "ean");
    Field price = new TableField(this.storeName, "price");
    Field date = new TableField(this.storeName, "date");

    Measure vector = new VectorAggMeasure("vectorPrice", price, AVG, date);
    Measure vectorDate = new VectorAggMeasure("vectorDate", date, "any_value", date);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(competitor, ean), List.of(vector, vectorDate))
//            .rollup(List.of(competitor, ean))
            .build();
    Table result = this.executor.executeQuery(query);
    result.show();
  }
}
