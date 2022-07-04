package me.paulbares.query;

import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.dto.NewQueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestBinaryOperationMeasure {

  protected Datastore datastore;

  protected QueryEngine queryEngine;

  protected NewQueryExecutor executor;

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
    this.executor = new NewQueryExecutor(this.queryEngine);
    this.tm = createTransactionManager();

    beforeLoading(List.of(ean, category, sales, qty));

    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"bottle", "drink", 20d, 10},
            new Object[]{"bottle", "drink", 10d, 5},

            new Object[]{"cookie", "food", 60d, 20},
            new Object[]{"cookie", "food", 30d, 10}
    ));
  }

  protected void beforeLoading(List<Field> fields) {
  }

  @Test
  void testPlus() {
    AggregatedMeasure sales = new AggregatedMeasure("sales", AggregationFunction.SUM);
    AggregatedMeasure quantity = new AggregatedMeasure("quantity", AggregationFunction.SUM);

    var query = new NewQueryDto()
            .table(this.storeName)
            .withMetric(new BinaryOperationMeasure("plus1", BinaryOperationMeasure.Operator.PLUS, sales, sales))
            .withMetric(new BinaryOperationMeasure("plus2", BinaryOperationMeasure.Operator.PLUS, sales, quantity))
            .withMetric(new BinaryOperationMeasure("plus3", BinaryOperationMeasure.Operator.PLUS, quantity, quantity));

    Table execute = this.executor.execute(query);
    System.out.println(execute);
  }
}
