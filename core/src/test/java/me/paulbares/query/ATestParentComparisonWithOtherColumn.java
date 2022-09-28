package me.paulbares.query;

import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.List;

import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class ATestParentComparisonWithOtherColumn {

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
  void testSimple() {
    Measure amount = QueryBuilder.sum("amount", "amount");
    QueryDto query = QueryBuilder.query()
            .table(this.storeName)
            .withColumn("continent")
            .withColumn("country")
            .withColumn("city")
            .withMeasure(amount);

    ParentComparisonMeasure pOp = QueryBuilder.parentComparison("percentOfParent", ComparisonMethod.DIVIDE, amount, List.of("city", "country", "continent"));

    query.withMeasure(pOp);

    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("eu", "france", "lyon", 3.1d, 3.1d / (4d + 3.1d)),
            Arrays.asList("eu", "france", "paris", 4d, 4d / (4d + 3.1d)),
            Arrays.asList("eu", "uk", "london", 9d, 1d));
  }

  @Test
  void testSkipMiddleAncestors() {
    Measure amount = QueryBuilder.sum("amount", "amount");
    QueryDto query = QueryBuilder.query()
            .table(this.storeName)
            .withColumn("continent")
            .withColumn("country")
            .withColumn("city")
            .withMeasure(amount);

    ParentComparisonMeasure pOp = QueryBuilder.parentComparison("percentOfParent", ComparisonMethod.DIVIDE, amount, List.of("city", "continent"));

    query.withMeasure(pOp);

    Table result = this.executor.execute(query);
    // Note: contrary to what you might expect, the result here is the same than having List.of("city", "country","continent")
    // and it is not meant to compute the percent of the grand parent.
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("eu", "france", "lyon", 3.1d, 3.1d / (4d + 3.1d)),
            Arrays.asList("eu", "france", "paris", 4d, 4d / (4d + 3.1d)),
            Arrays.asList("eu", "uk", "london", 9d, 1d));
  }

  @Test
  void testCrossjoinWithOtherColumn() {
    Measure amount = QueryBuilder.sum("amount", "amount");
    QueryDto query = QueryBuilder.query()
            .table(this.storeName)
            .withColumn("spending_category")
            .withColumn("continent")
            .withColumn("country")
            .withColumn("city")
            .withMeasure(amount);

    ParentComparisonMeasure pOp = QueryBuilder.parentComparison("percentOfParent", ComparisonMethod.DIVIDE, amount, List.of("city", "country", "continent"));

    query.withMeasure(pOp);

    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("car", "eu", "france", "lyon", 0.1d, 0.1d / (0.1d + 1d)),
            Arrays.asList("car", "eu", "france", "paris", 1d, 1d / (0.1d + 1d)),
            Arrays.asList("car", "eu", "uk", "london", 2d, 1d),
            Arrays.asList("hobbies", "eu", "france", "lyon", 1d, 1d / (1 + 1)),
            Arrays.asList("hobbies", "eu", "france", "paris", 1d, 1d / (1 + 1)),
            Arrays.asList("hobbies", "eu", "uk", "london", 5d, 1d),
            Arrays.asList("home", "eu", "france", "lyon", 2d, 2d / (2 + 2)),
            Arrays.asList("home", "eu", "france", "paris", 2d, 2d / (2 + 2)),
            Arrays.asList("home", "eu", "uk", "london", 2d, 1d));
  }

  @Test
  void testCrossjoinWithOtherColumnAndMissingAncestorsInQuery() {
    Measure amount = QueryBuilder.sum("amount", "amount");
    QueryDto query = QueryBuilder.query()
            .table(this.storeName)
            .withColumn("spending_category")
            .withColumn("city")
            .withMeasure(amount);

    ParentComparisonMeasure pOp = QueryBuilder.parentComparison("percentOfParent", ComparisonMethod.DIVIDE, amount, List.of("city", "country", "continent"));

    query.withMeasure(pOp);

    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("car", "london", 2d, 2d / (0.1d + 1d + 2d)),
            Arrays.asList("car", "lyon", 0.1d, 0.1d / (0.1d + 1d + 2d)),
            Arrays.asList("car", "paris", 1d, 1d / (0.1d + 1d + 2d)),
            Arrays.asList("hobbies", "london", 5d, 5d / (1 + 1 + 5)),
            Arrays.asList("hobbies", "lyon", 1d, 1d / (1 + 1 + 5)),
            Arrays.asList("hobbies", "paris", 1d, 1d / (1 + 1 + 5)),
            Arrays.asList("home", "london", 2d, 2d / (2 + 2 + 2)),
            Arrays.asList("home", "lyon", 2d, 2d / (2 + 2 + 2)),
            Arrays.asList("home", "paris", 2d, 2d / (2 + 2 + 2)));
  }

  // TODO test with conditions
  // TODO do a test that compare a measure computed by AITM
}
