package me.paulbares.query;

import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestBinaryOperationMeasure {

  protected Datastore datastore;

  protected QueryEngine queryEngine;

  protected QueryExecutor executor;

  protected TransactionManager tm;

  protected String storeName = "myAwesomeStore";

  protected abstract QueryEngine createQueryEngine(Datastore datastore);

  protected abstract Datastore createDatastore();

  protected abstract TransactionManager createTransactionManager();

  @BeforeAll
  void setup() {
    Field ean = new Field("ean", String.class);
    Field category = new Field("category", String.class);
    Field sales = new Field("sales", double.class);
    Field qty = new Field("quantity", long.class);

    this.datastore = createDatastore();
    this.queryEngine = createQueryEngine(this.datastore);
    this.executor = new QueryExecutor(this.queryEngine);
    this.tm = createTransactionManager();

    beforeLoading(List.of(ean, category, sales, qty));

    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"bottle", "drink", 20d, 10},
            new Object[]{"cookie", "food", 30d, 10}
    ));
  }

  protected void beforeLoading(List<Field> fields) {
  }

  /**
   * Try with measure of different types.
   */
  @Test
  void testPlus() {
    AggregatedMeasure sales = new AggregatedMeasure("sales", AggregationFunction.SUM);
    AggregatedMeasure quantity = new AggregatedMeasure("quantity", AggregationFunction.SUM);

    var query = new QueryDto()
            .table(this.storeName)
            .withMetric(sales)
            .withMetric(quantity)
            .withMetric(new BinaryOperationMeasure("plus1", Operator.PLUS, sales, sales))
            .withMetric(new BinaryOperationMeasure("plus2", Operator.PLUS, sales, quantity))
            .withMetric(new BinaryOperationMeasure("plus3", Operator.PLUS, quantity, quantity));

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
    AggregatedMeasure sales = new AggregatedMeasure("sales", AggregationFunction.SUM);
    AggregatedMeasure quantity = new AggregatedMeasure("quantity", AggregationFunction.SUM);

    var query = new QueryDto()
            .table(this.storeName)
            .withMetric(sales)
            .withMetric(quantity)
            .withMetric(new BinaryOperationMeasure("minus1", Operator.MINUS, sales, sales))
            .withMetric(new BinaryOperationMeasure("minus2", Operator.MINUS, sales, quantity))
            .withMetric(new BinaryOperationMeasure("minus3", Operator.MINUS, quantity, quantity));

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
    AggregatedMeasure sales = new AggregatedMeasure("sales", AggregationFunction.SUM);
    AggregatedMeasure quantity = new AggregatedMeasure("quantity", AggregationFunction.SUM);

    var query = new QueryDto()
            .table(this.storeName)
            .withMetric(sales)
            .withMetric(quantity)
            .withMetric(new BinaryOperationMeasure("multiply1", Operator.MULTIPLY, sales, sales))
            .withMetric(new BinaryOperationMeasure("multiply2", Operator.MULTIPLY, sales, quantity))
            .withMetric(new BinaryOperationMeasure("multiply3", Operator.MULTIPLY, quantity, quantity));

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
    AggregatedMeasure sales = new AggregatedMeasure("sales", AggregationFunction.SUM);
    AggregatedMeasure quantity = new AggregatedMeasure("quantity", AggregationFunction.SUM);

    var query = new QueryDto()
            .table(this.storeName)
            .withMetric(sales)
            .withMetric(quantity)
            .withMetric(new BinaryOperationMeasure("divide1", Operator.DIVIDE, sales, sales))
            .withMetric(new BinaryOperationMeasure("divide2", Operator.DIVIDE, sales, quantity))
            .withMetric(new BinaryOperationMeasure("divide3", Operator.DIVIDE, quantity, quantity));

    Table table = this.executor.execute(query);
    double salesV = 50d;
    long qtyV = 20l;
    Assertions.assertThat(table).contains(List.of(salesV, qtyV, salesV / salesV, salesV / qtyV, (double) qtyV / qtyV));
    Assertions
            .assertThat(table.headers().stream().map(Field::name))
            .containsExactlyInAnyOrder("sum(sales)", "sum(quantity)", "divide1", "divide2", "divide3");
  }
}
