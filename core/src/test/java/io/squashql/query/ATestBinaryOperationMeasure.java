package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.agg.AggregationFunction;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

import static io.squashql.transaction.TransactionManager.MAIN_SCENARIO_NAME;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestBinaryOperationMeasure extends ABaseTestQuery {

  protected String storeName = "store" + getClass().getSimpleName().toLowerCase();

  @Override
  protected Map<String, List<Field>> getFieldsByStore() {
    Field ean = new Field(this.storeName, "ean", String.class);
    Field category = new Field(this.storeName, "category", String.class);
    Field sales = new Field(this.storeName, "sales", double.class);
    Field qty = new Field(this.storeName, "quantity", long.class);
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

    Table table = this.executor.execute(query);
    double salesV = 50d;
    long qtyV = 20l;
    Assertions.assertThat(table).contains(List.of(salesV, qtyV, salesV + salesV, salesV + qtyV, qtyV + qtyV));
    Assertions
            .assertThat(table.headers().stream().map(Header::field).map(Field::name))
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

    Table table = this.executor.execute(query);
    double salesV = 50d;
    long qtyV = 20l;
    Assertions.assertThat(table).contains(List.of(salesV, qtyV, salesV - salesV, salesV - qtyV, qtyV - qtyV));
    Assertions
            .assertThat(table.headers().stream().map(Header::field).map(Field::name))
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

    Table table = this.executor.execute(query);
    double salesV = 50d;
    long qtyV = 20l;
    Assertions.assertThat(table).contains(List.of(salesV, qtyV, salesV * salesV, salesV * qtyV, qtyV * qtyV));
    Assertions
            .assertThat(table.headers().stream().map(Header::field).map(Field::name))
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

    Table table = this.executor.execute(query);
    double salesV = 50d;
    long qtyV = 20l;
    Assertions.assertThat(table).contains(List.of(salesV, qtyV, salesV / salesV, salesV / qtyV, (double) qtyV / qtyV));
    Assertions
            .assertThat(table.headers().stream().map(Header::field).map(Field::name))
            .containsExactlyInAnyOrder("sum(sales)", "sum(quantity)", "divide1", "divide2", "divide3");
  }
}
