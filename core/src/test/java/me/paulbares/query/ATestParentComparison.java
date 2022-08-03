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
public abstract class ATestParentComparison {

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
    Field population = new Field("population", int.class);

    this.datastore = createDatastore();
    this.queryEngine = createQueryEngine(this.datastore);
    this.executor = new QueryExecutor(this.queryEngine);
    this.tm = createTransactionManager();

    beforeLoading(List.of(city, country, continent, population));

    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"paris", "france", "eu", 2},
            new Object[]{"london", "uk", "eu", 9},
            new Object[]{"nyc", "usa", "am", 8},
            new Object[]{"chicago", "usa", "am", 3},
            new Object[]{"toronto", "canada", "am", 3},
            new Object[]{"montreal", "canada", "am", 2},
            new Object[]{"otawa", "canada", "am", 1}
    ));
  }

  protected void beforeLoading(List<Field> fields) {
  }

  @Test
  void test() {
    QueryDto query = QueryBuilder.query()
            .table(this.storeName)
            .withColumn("continent")
            .withColumn("country")
            .withColumn("city")
            .withMeasure(QueryBuilder.sum("population"));

    QueryBuilder.parentComparison("percentOfParent",
            ComparisonMethod.DIVIDE,
            QueryBuilder.sum("population"),
            List.of("city", "country", "continent"));

    Table result = this.executor.execute(query);
    result.show();
  }
}
