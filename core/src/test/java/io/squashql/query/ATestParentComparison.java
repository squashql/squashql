package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.PivotTableQueryDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.PivotTable;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.squashql.query.ComparisonMethod.DIVIDE;
import static io.squashql.query.Functions.*;
import static io.squashql.query.TableField.tableFields;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestParentComparison extends ABaseTestQuery {

  private final String storeName = "store" + getClass().getSimpleName().toLowerCase();
  private final TableField city = new TableField(this.storeName, "city");
  private final TableField country = new TableField(this.storeName, "country");
  private final TableField continent = new TableField(this.storeName, "continent");
  private final TableField population = new TableField(this.storeName, "population");

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField city = new TableTypedField(this.storeName, "city", String.class);
    TableTypedField country = new TableTypedField(this.storeName, "country", String.class);
    TableTypedField continent = new TableTypedField(this.storeName, "continent", String.class);
    TableTypedField population = new TableTypedField(this.storeName, "population", double.class);
    return Map.of(this.storeName, List.of(city, country, continent, population));
  }

  @Override
  protected void loadData() {
    this.tm.load(this.storeName, List.of(
            new Object[]{"paris", "france", "eu", 2d},
            new Object[]{"lyon", "france", "eu", 1d},
            new Object[]{"monaco", null, "eu", 1d}, // to have a null value as a parent
            new Object[]{"london", "uk", "eu", 8d},
            new Object[]{"nyc", "usa", "am", 8d},
            new Object[]{"chicago", "usa", "am", 4d},
            new Object[]{"toronto", "canada", "am", 3d},
            new Object[]{"montreal", "canada", "am", 7d},
            new Object[]{"otawa", "canada", "am", 2d}
    ));
  }

  @Test
  void testSimple() {
    Measure pop = Functions.sum("population", this.population);
    final List<Field> fields = List.of(this.continent, this.country, this.city);
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", DIVIDE, pop, fields);
    QueryDto query = Query
            .from(this.storeName)
            .select(fields, List.of(pop, pOp))
            .build();

    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("am", "canada", "montreal", 7d, .5833333333333334),
            Arrays.asList("am", "canada", "otawa", 2d, .16666666666666666),
            Arrays.asList("am", "canada", "toronto", 3d, 0.25),
            Arrays.asList("am", "usa", "chicago", 4d, .3333333333333333),
            Arrays.asList("am", "usa", "nyc", 8d, .6666666666666666),
            Arrays.asList("eu", "france", "lyon", 1d, .3333333333333333),
            Arrays.asList("eu", "france", "paris", 2d, .6666666666666666),
            Arrays.asList("eu", "uk", "london", 8d, 1d),
            Arrays.asList("eu", null, "monaco", 1d, 1d));

    query = Query
            .from(this.storeName)
            .select(List.of(this.continent, this.country, this.city), List.of(pOp))
            .build(); // query only parent

    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("am", "canada", "montreal", .5833333333333334),
            Arrays.asList("am", "canada", "otawa", .16666666666666666),
            Arrays.asList("am", "canada", "toronto", 0.25),
            Arrays.asList("am", "usa", "chicago", .3333333333333333),
            Arrays.asList("am", "usa", "nyc", .6666666666666666),
            Arrays.asList("eu", "france", "lyon", .3333333333333333),
            Arrays.asList("eu", "france", "paris", .6666666666666666),
            Arrays.asList("eu", "uk", "london", 1d),
            Arrays.asList("eu", null, "monaco", 1d));
  }

  /**
   * This test looks like {@link #testSimple()} but uses the PT API that uses grouping sets instead of rollup.
   */
  @Test
  void testSimplePivotTable() {
    Measure pop = Functions.sum("population", this.population);
    final List<Field> fields = List.of(this.continent, this.country, this.city);
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", DIVIDE, pop, fields);
    QueryDto query = Query
            .from(this.storeName)
            .select(fields, List.of(pop, pOp))
            .build();

    PivotTable pivotTable = this.executor.executePivotQuery(new PivotTableQueryDto(query, fields, List.of(), fields));
    Assertions.assertThat(pivotTable.table).containsExactly(
            Arrays.asList("am", "canada", "montreal", 7d, .5833333333333334),
            Arrays.asList("am", "canada", "otawa", 2d, .16666666666666666),
            Arrays.asList("am", "canada", "toronto", 3d, 0.25),
            Arrays.asList("am", "usa", "chicago", 4d, .3333333333333333),
            Arrays.asList("am", "usa", "nyc", 8d, .6666666666666666),
            Arrays.asList("eu", "france", "lyon", 1d, .3333333333333333),
            Arrays.asList("eu", "france", "paris", 2d, .6666666666666666),
            Arrays.asList("eu", "uk", "london", 8d, 1d),
            Arrays.asList("eu", null, "monaco", 1d, 1d));
    }

  @Test
  void testClearFilter() {
    Measure pop = Functions.sum("population", this.population);
    List<Field> fields = List.of(this.continent, this.country, this.city);
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", DIVIDE, pop, fields);
    QueryDto query = Query
            .from(this.storeName)
            .where(criterion(this.city, in("montreal", "toronto")))
            .select(fields, List.of(pOp))
            .build(); // query only parent

    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("am", "canada", "montreal", .5833333333333334),
            Arrays.asList("am", "canada", "toronto", 0.25));

    query = Query
            .from(this.storeName)
            .where(criterion(this.country, eq("canada")))
            .select(List.of(this.continent, this.country, this.city), List.of(pOp))
            .build(); // query only parent

    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("am", "canada", "montreal", .5833333333333334),
            Arrays.asList("am", "canada", "otawa", .16666666666666666),
            Arrays.asList("am", "canada", "toronto", 0.25));

    query = Query
            .from(this.storeName)
            .where(criterion(this.continent, eq("eu")))
            .select(List.of(this.continent, this.country, this.city), List.of(pOp))
            .build(); // query only parent
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("eu", "france", "lyon", .3333333333333333),
            Arrays.asList("eu", "france", "paris", .6666666666666666),
            Arrays.asList("eu", "uk", "london", 1d),
            Arrays.asList("eu", null, "monaco", 1d));
  }

  @Test
  void testFiltersNotCleared() {
    Measure pop = Functions.sum("population", this.population);
    List<Field> fields = List.of(this.continent, this.country, this.city);
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", DIVIDE, pop, fields);
    pOp.clearFilters = false;

    QueryDto query = Query
            .from(this.storeName)
            .where(criterion(this.country, eq("canada")))
            .select(List.of(this.country, this.city), List.of(pOp))
            .rollup(List.of(this.country, this.city))
            .build(); // query only parent

    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList(GRAND_TOTAL, GRAND_TOTAL, 1d),
            Arrays.asList("canada", TOTAL, 1d),
            Arrays.asList("canada", "montreal", .5833333333333334d),
            Arrays.asList("canada", "otawa", .16666666666666666d),
            Arrays.asList("canada", "toronto", 0.25d)
    );
  }

  @Test
  void testWithMissingAncestor() {
    Measure pop = Functions.sum("population", this.population);
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", DIVIDE, pop, tableFields(List.of("country", "continent")));
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(this.continent, this.country, this.city), List.of(pop, pOp))
            .build(); // query only parent

    Table result = this.executor.executeQuery(query);
    // Always 1 because the parent scope will be [city, continent] so each cell value is compared to itself.
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("am", "canada", "montreal", 7d, 1d),
            Arrays.asList("am", "canada", "otawa", 2d, 1d),
            Arrays.asList("am", "canada", "toronto", 3d, 1d),
            Arrays.asList("am", "usa", "chicago", 4d, 1d),
            Arrays.asList("am", "usa", "nyc", 8d, 1d),
            Arrays.asList("eu", "france", "lyon", 1d, 1d),
            Arrays.asList("eu", "france", "paris", 2d, 1d),
            Arrays.asList("eu", "uk", "london", 8d, 1d),
            Arrays.asList("eu", null, "monaco", 1d, 1d));
  }

  @Test
  void testWithCalculatedMeasure() {
    // Note this calculation may look weird but the goal of this test is to make sure we can compute the percent of
    // parent with a measure computed by SquashQL.
    Measure pop = Functions.sum("populationsum", this.population);
    Measure pop2 = Functions.plus("pop2", pop, Functions.integer(2));
    List<Field> select = List.of(this.continent, this.country, this.city);
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", DIVIDE, pop, select);
    ComparisonMeasureReferencePosition pOp2 = new ComparisonMeasureReferencePosition("percentOfParent2", DIVIDE, pop2, select);
    QueryDto query = Query
            .from(this.storeName)
            .where(criterion(this.country, eq("canada"))) // use a filter to limit the number of rows
            .select(select, List.of(pop, pOp, pop2, pOp2))
            .build(); // query only parent

    Table table = this.executor.executeQuery(query);
    double canadaTotalPlus2 = 7d + 2d + 3d + 2;
    Assertions.assertThat(table).containsExactly(
            Arrays.asList("am", "canada", "montreal", 7d, .5833333333333334, 7d + 2, (7d + 2) / canadaTotalPlus2),
            Arrays.asList("am", "canada", "otawa", 2d, .16666666666666666, 2d + 2, (2d + 2) / canadaTotalPlus2),
            Arrays.asList("am", "canada", "toronto", 3d, 0.25, 3d + 2, (3d + 2) / canadaTotalPlus2));
  }

  @Test
  void testSimpleWithTotals() {
    Measure pop = Functions.sum("population", this.population);
    List<Field> fields = List.of(this.continent, this.country, this.city);
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", DIVIDE, pop, fields);
    QueryDto query = Query
            .from(this.storeName)
            .select(fields, List.of(pop, pOp))
            .rollup(fields)
            .build();

    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList(GRAND_TOTAL, GRAND_TOTAL, GRAND_TOTAL, 36d, 1d),
            Arrays.asList("am", TOTAL, TOTAL, 24d, .6666666666666666),
            Arrays.asList("am", "canada", TOTAL, 12d, .5),
            Arrays.asList("am", "canada", "montreal", 7d, .5833333333333334),
            Arrays.asList("am", "canada", "otawa", 2d, .16666666666666666),
            Arrays.asList("am", "canada", "toronto", 3d, 0.25),
            Arrays.asList("am", "usa", TOTAL, 12d, .5),
            Arrays.asList("am", "usa", "chicago", 4d, .3333333333333333),
            Arrays.asList("am", "usa", "nyc", 8d, .6666666666666666),
            Arrays.asList("eu", TOTAL, TOTAL, 12d, .3333333333333333),
            Arrays.asList("eu", "france", TOTAL, 3d, .25),
            Arrays.asList("eu", "france", "lyon", 1d, .3333333333333333),
            Arrays.asList("eu", "france", "paris", 2d, .6666666666666666),
            Arrays.asList("eu", "uk", TOTAL, 8d, .6666666666666666),
            Arrays.asList("eu", "uk", "london", 8d, 1d),
            Arrays.asList("eu", null, TOTAL, 1d, .08333333333333333),
            Arrays.asList("eu", null, "monaco", 1d, 1d));
  }
}
