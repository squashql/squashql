package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.agg.AggregationFunction;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.measure.AggregatedMeasure;
import io.squashql.query.measure.BinaryOperationMeasure;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

import static io.squashql.transaction.DataLoader.MAIN_SCENARIO_NAME;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestBinaryOperationMeasure extends ABaseTestQuery {

  private final String storeName = "store" + getClass().getSimpleName().toLowerCase();

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField ean = new TableTypedField(this.storeName, "ean", String.class);
    TableTypedField category = new TableTypedField(this.storeName, "category", String.class);
    TableTypedField sales = new TableTypedField(this.storeName, "sales", double.class);
    TableTypedField qty = new TableTypedField(this.storeName, "quantity", long.class);
    return Map.of(this.storeName, List.of(ean, category, sales, qty));
  }

  @Override
  protected void loadData() {
    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"bottle", "drink", 20d, 10l},
            new Object[]{"cookie", "food", 30d, 10l}
    ));
  }

  /**
   * Try with measure of different types.
   */
  @Test
  void testPlus() {
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", AggregationFunction.SUM);
    AggregatedMeasure quantity = new AggregatedMeasure("sum(quantity)", "quantity", AggregationFunction.SUM);

    var query = new QueryDto()
            .table(this.storeName)
            .withMeasure(sales)
            .withMeasure(quantity)
            .withMeasure(new BinaryOperationMeasure("plus1", BinaryOperator.PLUS, sales, sales))
            .withMeasure(new BinaryOperationMeasure("plus2", BinaryOperator.PLUS, sales, quantity))
            .withMeasure(new BinaryOperationMeasure("plus3", BinaryOperator.PLUS, quantity, quantity));

    Table table = this.executor.executeQuery(query);
    double salesV = 50d;
    long qtyV = 20l;
    Assertions.assertThat(table).contains(List.of(salesV, qtyV, salesV + salesV, salesV + qtyV, qtyV + qtyV));
    Assertions
            .assertThat(table.headers().stream().map(Header::name))
            .containsExactlyInAnyOrder("sum(sales)", "sum(quantity)", "plus1", "plus2", "plus3");
  }

  /**
   * Try with measure of different types.
   */
  @Test
  void testMinus() {
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", AggregationFunction.SUM);
    AggregatedMeasure quantity = new AggregatedMeasure("sum(quantity)", "quantity", AggregationFunction.SUM);

    var query = new QueryDto()
            .table(this.storeName)
            .withMeasure(sales)
            .withMeasure(quantity)
            .withMeasure(new BinaryOperationMeasure("minus1", BinaryOperator.MINUS, sales, sales))
            .withMeasure(new BinaryOperationMeasure("minus2", BinaryOperator.MINUS, sales, quantity))
            .withMeasure(new BinaryOperationMeasure("minus3", BinaryOperator.MINUS, quantity, quantity));

    Table table = this.executor.executeQuery(query);
    double salesV = 50d;
    long qtyV = 20l;
    Assertions.assertThat(table).contains(List.of(salesV, qtyV, salesV - salesV, salesV - qtyV, qtyV - qtyV));
    Assertions
            .assertThat(table.headers().stream().map(Header::name))
            .containsExactlyInAnyOrder("sum(sales)", "sum(quantity)", "minus1", "minus2", "minus3");
  }

  /**
   * Try with measure of different types.
   */
  @Test
  void testMultiply() {
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", AggregationFunction.SUM);
    AggregatedMeasure quantity = new AggregatedMeasure("sum(quantity)", "quantity", AggregationFunction.SUM);

    var query = new QueryDto()
            .table(this.storeName)
            .withMeasure(sales)
            .withMeasure(quantity)
            .withMeasure(new BinaryOperationMeasure("multiply1", BinaryOperator.MULTIPLY, sales, sales))
            .withMeasure(new BinaryOperationMeasure("multiply2", BinaryOperator.MULTIPLY, sales, quantity))
            .withMeasure(new BinaryOperationMeasure("multiply3", BinaryOperator.MULTIPLY, quantity, quantity));

    Table table = this.executor.executeQuery(query);
    double salesV = 50d;
    long qtyV = 20l;
    Assertions.assertThat(table).contains(List.of(salesV, qtyV, salesV * salesV, salesV * qtyV, qtyV * qtyV));
    Assertions
            .assertThat(table.headers().stream().map(Header::name))
            .containsExactlyInAnyOrder("sum(sales)", "sum(quantity)", "multiply1", "multiply2", "multiply3");
  }

  /**
   * Try with measure of different types.
   */
  @Test
  void testDivide() {
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", AggregationFunction.SUM);
    AggregatedMeasure quantity = new AggregatedMeasure("sum(quantity)", "quantity", AggregationFunction.SUM);

    var query = new QueryDto()
            .table(this.storeName)
            .withMeasure(sales)
            .withMeasure(quantity)
            .withMeasure(new BinaryOperationMeasure("divide1", BinaryOperator.DIVIDE, sales, sales))
            .withMeasure(new BinaryOperationMeasure("divide2", BinaryOperator.DIVIDE, sales, quantity))
            .withMeasure(new BinaryOperationMeasure("divide3", BinaryOperator.DIVIDE, quantity, quantity));

    Table table = this.executor.executeQuery(query);
    double salesV = 50d;
    long qtyV = 20l;
    Assertions.assertThat(table).contains(List.of(salesV, qtyV, salesV / salesV, salesV / qtyV, (double) qtyV / qtyV));
    Assertions
            .assertThat(table.headers().stream().map(Header::name))
            .containsExactlyInAnyOrder("sum(sales)", "sum(quantity)", "divide1", "divide2", "divide3");
  }

  /**
   * Try with measure of different types.
   */
  @Test
  void testRelativeDifference() {
    AggregatedMeasure sales = new AggregatedMeasure("sum(sales)", "sales", AggregationFunction.SUM);
    AggregatedMeasure quantity = new AggregatedMeasure("sum(quantity)", "quantity", AggregationFunction.SUM);

    QueryDto query = Query.from(this.storeName)
            .select(List.of(),
                    List.of(),
                    List.of(new BinaryOperationMeasure("rel diff", BinaryOperator.RELATIVE_DIFFERENCE, sales, quantity)))
            .build();

    Table table = this.executor.executeQuery(query);
    double salesV = 50d;
    long qtyV = 20l;
    Assertions.assertThat(table).contains(List.of((salesV - qtyV) / qtyV));
  }
}
