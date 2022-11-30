package me.paulbares.query;

import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

import static me.paulbares.query.BinaryOperator.*;
import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@ExtendWith(TestBinaryOperationMeasure.CustomTestInstanceFactory.class)
@TestConfigurers({ClickHouseTestConfigurer.class})
public class TestBinaryOperationMeasure extends ATestQuery {

//  public TestBinaryOperationMeasure(TestConfigurer configurer) {
//    super(configurer);
//  }
//
//  public static class CustomTestInstanceFactory implements TestInstanceFactory {
//
//    @Override
//    public Object createTestInstance(TestInstanceFactoryContext factoryContext, ExtensionContext extensionContext) throws TestInstantiationException {
//      Class<?> testClass = factoryContext.getTestClass();
//      Optional<Object> outerInstance = factoryContext.getOuterInstance();
//      extensionContext.
//      return null;
//    }
//  }

  protected String storeName = "myAwesomeStore";

  @Override
  protected Map<String, List<Field>> getFieldsByStore() {
    Field ean = new Field("ean", String.class);
    Field category = new Field("category", String.class);
    Field sales = new Field("sales", double.class);
    Field qty = new Field("quantity", long.class);
    return Map.of(this.storeName, List.of(ean, category, sales, qty));
  }

  @Override
  protected void loadData(TransactionManager tm) {
    tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
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
            .withMeasure(new BinaryOperationMeasure("plus1", PLUS, sales, sales))
            .withMeasure(new BinaryOperationMeasure("plus2", PLUS, sales, quantity))
            .withMeasure(new BinaryOperationMeasure("plus3", PLUS, quantity, quantity));

    Table table = this.executor.execute(query);
    double salesV = 50d;
    long qtyV = 20l;
    Assertions.assertThat(table).contains(List.of(salesV, qtyV, salesV + salesV, salesV + qtyV, qtyV + qtyV));
    Assertions
            .assertThat(table.headers().stream().map(Field::name))
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
            .withMeasure(new BinaryOperationMeasure("minus1", MINUS, sales, sales))
            .withMeasure(new BinaryOperationMeasure("minus2", MINUS, sales, quantity))
            .withMeasure(new BinaryOperationMeasure("minus3", MINUS, quantity, quantity));

    Table table = this.executor.execute(query);
    double salesV = 50d;
    long qtyV = 20l;
    Assertions.assertThat(table).contains(List.of(salesV, qtyV, salesV - salesV, salesV - qtyV, qtyV - qtyV));
    Assertions
            .assertThat(table.headers().stream().map(Field::name))
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
            .withMeasure(new BinaryOperationMeasure("multiply1", MULTIPLY, sales, sales))
            .withMeasure(new BinaryOperationMeasure("multiply2", MULTIPLY, sales, quantity))
            .withMeasure(new BinaryOperationMeasure("multiply3", MULTIPLY, quantity, quantity));

    Table table = this.executor.execute(query);
    double salesV = 50d;
    long qtyV = 20l;
    Assertions.assertThat(table).contains(List.of(salesV, qtyV, salesV * salesV, salesV * qtyV, qtyV * qtyV));
    Assertions
            .assertThat(table.headers().stream().map(Field::name))
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
            .withMeasure(new BinaryOperationMeasure("divide1", DIVIDE, sales, sales))
            .withMeasure(new BinaryOperationMeasure("divide2", DIVIDE, sales, quantity))
            .withMeasure(new BinaryOperationMeasure("divide3", DIVIDE, quantity, quantity));

    Table table = this.executor.execute(query);
    double salesV = 50d;
    long qtyV = 20l;
    Assertions.assertThat(table).contains(List.of(salesV, qtyV, salesV / salesV, salesV / qtyV, (double) qtyV / qtyV));
    Assertions
            .assertThat(table.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder("sum(sales)", "sum(quantity)", "divide1", "divide2", "divide3");
  }
}
