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

import static me.paulbares.query.QueryBuilder.eq;
import static me.paulbares.query.QueryBuilder.in;
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
    Field population = new Field("population", double.class);

    this.datastore = createDatastore();
    this.queryEngine = createQueryEngine(this.datastore);
    this.executor = new QueryExecutor(this.queryEngine);
    this.tm = createTransactionManager();

    beforeLoading(List.of(city, country, continent, population));

    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"paris", "france", "eu", 2},
            new Object[]{"lyon", "france", "eu", 0.5},
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
  void testSimple() {
    Measure pop = QueryBuilder.sum("population", "population");
    QueryDto query = QueryBuilder.query()
            .table(this.storeName)
            .withColumn("continent")
            .withColumn("country")
            .withColumn("city")
            .withMeasure(pop);

    // If no ancestors is expressed in the query, return 1.
    ParentComparisonMeasure pOp = QueryBuilder.parentComparison("percentOfParent", ComparisonMethod.DIVIDE, pop, List.of("city", "country", "continent"));
    query.withMeasure(pOp);

    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("am", "canada", "montreal", 2d, .3333333333333333),
            Arrays.asList("am", "canada", "otawa", 1d, .16666666666666666),
            Arrays.asList("am", "canada", "toronto", 3d, 0.5),
            Arrays.asList("am", "usa", "chicago", 3d, .2727272727272727),
            Arrays.asList("am", "usa", "nyc", 8d, .7272727272727273),
            Arrays.asList("eu", "france", "lyon", 0.5, 0.2),
            Arrays.asList("eu", "france", "paris", 2d, 0.8),
            Arrays.asList("eu", "uk", "london", 9d, 1d));

    query = QueryBuilder.query()
            .table(this.storeName)
            .withColumn("continent")
            .withColumn("country")
            .withColumn("city")
            .withMeasure(pOp); // query only parent

    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("am", "canada", "montreal", .3333333333333333),
            Arrays.asList("am", "canada", "otawa", .16666666666666666),
            Arrays.asList("am", "canada", "toronto", 0.5),
            Arrays.asList("am", "usa", "chicago", .2727272727272727),
            Arrays.asList("am", "usa", "nyc", .7272727272727273),
            Arrays.asList("eu", "france", "lyon", 0.2),
            Arrays.asList("eu", "france", "paris", 0.8),
            Arrays.asList("eu", "uk", "london", 1d));
  }

  @Test
  void testClearFilter() {
    Measure pop = QueryBuilder.sum("population", "population");
    ParentComparisonMeasure pOp = QueryBuilder.parentComparison("percentOfParent", ComparisonMethod.DIVIDE, pop, List.of("city", "country", "continent"));
    QueryDto query = QueryBuilder.query()
            .table(this.storeName)
            .withColumn("continent")
            .withColumn("country")
            .withColumn("city")
            .withCondition("city", in("montreal", "toronto"))
            .withMeasure(pOp); // query only parent

    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("am", "canada", "montreal", .3333333333333333),
            Arrays.asList("am", "canada", "toronto", 0.5));

    query = QueryBuilder.query()
            .table(this.storeName)
            .withColumn("continent")
            .withColumn("country")
            .withColumn("city")
            .withCondition("country", eq("canada"))
            .withMeasure(pOp); // query only parent

    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("am", "canada", "montreal", .3333333333333333),
            Arrays.asList("am", "canada", "otawa", .16666666666666666),
            Arrays.asList("am", "canada", "toronto", 0.5));

    query = QueryBuilder.query()
            .table(this.storeName)
            .withColumn("continent")
            .withColumn("country")
            .withColumn("city")
            .withCondition("continent", eq("eu"))
            .withMeasure(pOp); // query only parent
    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("eu", "france", "lyon", 0.2),
            Arrays.asList("eu", "france", "paris", 0.8),
            Arrays.asList("eu", "uk", "london", 1d));
  }

  @Test
  void testWithMissingAncestor() {
    Measure pop = QueryBuilder.sum("population", "population");
    QueryDto query = QueryBuilder.query()
            .table(this.storeName)
            .withColumn("continent")
            .withColumn("country")
            .withColumn("city")
            .withMeasure(pop);

    ParentComparisonMeasure pOp = QueryBuilder.parentComparison("percentOfParent", ComparisonMethod.DIVIDE, pop, List.of("country", "continent"));

    query.withMeasure(pOp);

    Table result = this.executor.execute(query);
    // Always 1 because the parent scope will be [city, continent] so each cell value is compared to itself.
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("am", "canada", "montreal", 2d, 1d),
            Arrays.asList("am", "canada", "otawa", 1d, 1d),
            Arrays.asList("am", "canada", "toronto", 3d, 1d),
            Arrays.asList("am", "usa", "chicago", 3d, 1d),
            Arrays.asList("am", "usa", "nyc", 8d, 1d),
            Arrays.asList("eu", "france", "lyon", 0.5, 1d),
            Arrays.asList("eu", "france", "paris", 2d, 1d),
            Arrays.asList("eu", "uk", "london", 9d, 1d));
  }

  @Test
  void testWithCalculatedMeasure() {
    Measure pop = QueryBuilder.multiply("double", QueryBuilder.sum("population", "population"), QueryBuilder.integer(2));
    QueryDto query = QueryBuilder.query()
            .table(this.storeName)
            .withColumn("continent")
            .withColumn("country")
            .withColumn("city")
            .withMeasure(pop);

    ParentComparisonMeasure pOp = QueryBuilder.parentComparison("percentOfParent", ComparisonMethod.DIVIDE, pop, List.of("city", "country", "continent"));

    query.withMeasure(pOp);

    Assertions.assertThatThrownBy(() -> this.executor.execute(query))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("primitive measure");
  }

  // TODO test with conditions
}
