package me.paulbares.query;

import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.junit.jupiter.api.*;

import java.util.List;

import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class ATestParentComparison2 {

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
    Field city = new Field("city", String.class);
    Field country = new Field("country", String.class);
    Field continent = new Field("continent", String.class);
    Field spendingCategory = new Field("spending_category", String.class);
    Field amount = new Field("amount", double.class);

    this.datastore = createDatastore();
    this.queryEngine = createQueryEngine(this.datastore);
    this.executor = new QueryExecutor(this.queryEngine);
    this.tm = createTransactionManager();

    beforeLoading(List.of(city, country, continent, spendingCategory, amount));

    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"paris", "france", "eu", "car", 1},
            new Object[]{"paris", "france", "eu", "home", 2},
            new Object[]{"paris", "france", "eu", "hobbies", 1},
            new Object[]{"lyon", "france", "eu", "car", 0.1},
            new Object[]{"lyon", "france", "eu", "home", 2},
            new Object[]{"lyon", "france", "eu", "hobbies", 1},
            new Object[]{"london", "uk", "eu", "car", 2},
            new Object[]{"london", "uk", "eu", "home", 2},
            new Object[]{"london", "uk", "eu", "hobbies", 5}
    ));
  }

  protected void beforeLoading(List<Field> fields) {
  }

  @Test
  void test() {
    Measure amount = QueryBuilder.sum("amount", "amount");
    QueryDto query = QueryBuilder.query()
            .table(this.storeName)
            .withColumn("spending_category")
            .withColumn("continent")
            .withColumn("country")
            .withColumn("city")
            .withMeasure(amount);

    // If no ancestors is expressed in the query, return 1.
    ParentComparisonMeasure pOp = QueryBuilder.parentComparison("percentOfParent", ComparisonMethod.DIVIDE, amount, List.of("city", "country", "continent"));
    // When adding this measure to the query, new rows should appear. with subtotal and GT.

//    query.withMeasure(pOp);

    Table result = this.executor.execute(query);
    result.show();
  }

  // TODO test with other columns
  // TODO test with conditions
  // TODO do a test that compare a measure computed by AITM
}