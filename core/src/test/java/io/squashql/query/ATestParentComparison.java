package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.squashql.query.Functions.*;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;
import static io.squashql.transaction.TransactionManager.MAIN_SCENARIO_NAME;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestParentComparison extends ABaseTestQuery {

  protected String storeName = "store" + getClass().getSimpleName().toLowerCase();

  @Override
  protected Map<String, List<Field>> getFieldsByStore() {
    Field city = new Field("city", String.class);
    Field country = new Field("country", String.class);
    Field continent = new Field("continent", String.class);
    Field population = new Field("population", double.class);
    return Map.of(this.storeName, List.of(city, country, continent, population));
  }

  @Override
  protected void loadData() {
    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"paris", "france", "eu", 2d},
            new Object[]{"lyon", "france", "eu", 0.5},
            new Object[]{"london", "uk", "eu", 9d},
            new Object[]{"nyc", "usa", "am", 8d},
            new Object[]{"chicago", "usa", "am", 3d},
            new Object[]{"toronto", "canada", "am", 3d},
            new Object[]{"montreal", "canada", "am", 2d},
            new Object[]{"otawa", "canada", "am", 1d}
    ));
  }

  @Test
  void testSimple() {
    Measure pop = Functions.sum("population", "population");
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", ComparisonMethod.DIVIDE, pop, List.of("city", "country", "continent"));
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("continent", "country", "city"), List.of(pop, pOp))
            .build();

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

    query = Query
            .from(this.storeName)
            .select(List.of("continent", "country", "city"), List.of(pOp))
            .build(); // query only parent

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
    Measure pop = Functions.sum("population", "population");
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", ComparisonMethod.DIVIDE, pop, List.of("city", "country", "continent"));
    QueryDto query = Query
            .from(this.storeName)
            .where(criterion("city", in("montreal", "toronto")))
            .select(List.of("continent", "country", "city"), List.of(pOp))
            .build(); // query only parent

    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("am", "canada", "montreal", .3333333333333333),
            Arrays.asList("am", "canada", "toronto", 0.5));

    query = Query
            .from(this.storeName)
            .where(criterion("country", eq("canada")))
            .select(List.of("continent", "country", "city"), List.of(pOp))
            .build(); // query only parent

    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("am", "canada", "montreal", .3333333333333333),
            Arrays.asList("am", "canada", "otawa", .16666666666666666),
            Arrays.asList("am", "canada", "toronto", 0.5));

    query = Query
            .from(this.storeName)
            .where(criterion("continent", eq("eu")))
            .select(List.of("continent", "country", "city"), List.of(pOp))
            .build(); // query only parent
    result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("eu", "france", "lyon", 0.2),
            Arrays.asList("eu", "france", "paris", 0.8),
            Arrays.asList("eu", "uk", "london", 1d));
  }

  @Test
  void testWithMissingAncestor() {
    Measure pop = Functions.sum("population", "population");
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", ComparisonMethod.DIVIDE, pop, List.of("country", "continent"));
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("continent", "country", "city"), List.of(pop, pOp))
            .build(); // query only parent

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
    Measure pop = Functions.multiply("double", Functions.sum("population", "population"), Functions.integer(2));
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", ComparisonMethod.DIVIDE, pop, List.of("city", "country", "continent"));
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("continent", "country", "city"), List.of(pop, pOp))
            .build(); // query only parent

    Assertions.assertThatThrownBy(() -> this.executor.execute(query))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("primitive measure");
  }

  @Test
  void testSimpleWithTotals() {
    Measure pop = Functions.sum("population", "population");
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", ComparisonMethod.DIVIDE, pop, List.of("city", "country", "continent"));
    List<String> cols = List.of("continent", "country", "city");
    QueryDto query = Query
            .from(this.storeName)
            .select(cols, List.of(pop, pOp))
            .rollup(cols)
            .build();

    Table result = this.executor.execute(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList(GRAND_TOTAL, GRAND_TOTAL, GRAND_TOTAL, 28.5d, 1d),
            Arrays.asList("am", TOTAL, TOTAL, 17d, 0.5964912280701754d),
            Arrays.asList("am", "canada", TOTAL, 6d, 0.35294117647058826d),
            Arrays.asList("am", "canada", "montreal", 2d, .3333333333333333),
            Arrays.asList("am", "canada", "otawa", 1d, .16666666666666666),
            Arrays.asList("am", "canada", "toronto", 3d, 0.5),
            Arrays.asList("am", "usa", TOTAL, 11d, 0.6470588235294118d),
            Arrays.asList("am", "usa", "chicago", 3d, .2727272727272727),
            Arrays.asList("am", "usa", "nyc", 8d, .7272727272727273),
            Arrays.asList("eu", TOTAL, TOTAL, 11.5d, 0.40350877192982454d),
            Arrays.asList("eu", "france", TOTAL, 2.5, 0.21739130434782608),
            Arrays.asList("eu", "france", "lyon", 0.5, 0.2),
            Arrays.asList("eu", "france", "paris", 2d, 0.8),
            Arrays.asList("eu", "uk", TOTAL, 9d, 0.782608695652174),
            Arrays.asList("eu", "uk", "london", 9d, 1d));
  }
}
