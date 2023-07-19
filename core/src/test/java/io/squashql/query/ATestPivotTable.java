package io.squashql.query;

import com.google.common.collect.ImmutableList;
import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.dto.BucketColumnSetDto;
import io.squashql.query.dto.PivotTableQueryDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.TypedField;
import io.squashql.table.*;
import io.squashql.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Paths;
import java.util.*;

import static io.squashql.query.ComparisonMethod.ABSOLUTE_DIFFERENCE;
import static io.squashql.query.Functions.*;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;

@TestClass(ignore = {TestClass.Type.SNOWFLAKE})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestPivotTable extends ABaseTestQuery {

  protected String storeName = "store" + getClass().getSimpleName().toLowerCase();

  @Override
  protected Map<String, List<TypedField>> getFieldsByStore() {
    TypedField city = new TypedField(this.storeName, "city", String.class);
    TypedField country = new TypedField(this.storeName, "country", String.class);
    TypedField continent = new TypedField(this.storeName, "continent", String.class);
    TypedField spendingCategory = new TypedField(this.storeName, "spending category", String.class);
    TypedField spendingSubcategory = new TypedField(this.storeName, "spending subcategory", String.class);
    TypedField amount = new TypedField(this.storeName, "amount", double.class);
    return Map.of(this.storeName, List.of(city, country, continent, spendingCategory, spendingSubcategory, amount));
  }

  @Override
  protected void loadData() {
    this.tm.load(this.storeName, List.of(
            new Object[]{"paris", "france", "eu", "minimum expenditure", "car", 1d},
            new Object[]{"paris", "france", "eu", "minimum expenditure", "housing", 2d},
            new Object[]{"paris", "france", "eu", "extra", "hobbies", 1d},
            new Object[]{"lyon", "france", "eu", "minimum expenditure", "car", 1d},
            new Object[]{"lyon", "france", "eu", "minimum expenditure", "housing", 2d},
            new Object[]{"lyon", "france", "eu", "extra", "hobbies", 1d},
            new Object[]{"london", "uk", "eu", "minimum expenditure", "car", 2d},
            new Object[]{"london", "uk", "eu", "minimum expenditure", "housing", 2d},
            new Object[]{"london", "uk", "eu", "extra", "hobbies", 5d},

            new Object[]{"nyc", "usa", "am", "minimum expenditure", "car", 8d},
            new Object[]{"nyc", "usa", "am", "minimum expenditure", "housing", 12d},
            new Object[]{"nyc", "usa", "am", "extra", "hobbies", 6d},

            new Object[]{"la", "usa", "am", "minimum expenditure", "car", 7d},
            new Object[]{"la", "usa", "am", "minimum expenditure", "housing", 2d},
            new Object[]{"la", "usa", "am", "extra", "hobbies", 4d}
    ));
  }

  private static Table tableFromFile(TestInfo testInfo) {
    return TestUtil.deserializeTableFromFile(Paths.get("queryresults", "pivottable", testInfo.getTestMethod().get().getName() + ".tabular.json"));
  }

  private static List<List<Object>> pivotTableFromFile(TestInfo testInfo) {
    return TestUtil.deserializeFromFile(Paths.get("queryresults", "pivottable", testInfo.getTestMethod().get().getName() + ".pivottable.json"), List.class);
  }

  @Test
  void testRollupEquivalent() {
    Measure amount = Functions.sum("amount", "amount");

    QueryDto queryWithoutRollup = Query
            .from(this.storeName)
            .select(List.of("continent", "country", "city"), List.of(amount))
            .build();
    QueryDto queryRollup = Query
            .from(this.storeName)
            .select(List.of("continent", "country", "city"), List.of(amount))
            .rollup(List.of("continent", "country", "city"))
            .build();
    Table resultRollup = this.executor.execute(queryRollup);

    // NO COLUMNS
    {
      PivotTable result = this.executor.execute(new PivotTableQueryDto(queryWithoutRollup, List.of("continent", "country", "city"), List.of()));
      Assertions.assertThat(result.table).containsExactlyInAnyOrderElementsOf(resultRollup);
    }
    // NO ROWS
    {
      PivotTable result = this.executor.execute(new PivotTableQueryDto(queryWithoutRollup, List.of(), List.of("continent", "country", "city")));
      Assertions.assertThat(result.table).containsExactlyInAnyOrderElementsOf(resultRollup);
    }
  }

  /**
   * Simple case.
   */
  @Test
  void testOneColumnEachAxis() {
    Measure amount = Functions.sum("amount", "amount");

    QueryDto query = Query
            .from(this.storeName)
            .where(criterion("city", in("la", "london"))) // to reduce size of the output
            .select(List.of("spending category", "city"), List.of(amount))
            .build();
    List<String> rows = List.of("city");
    List<String> columns = List.of("spending category");
    PivotTable result = this.executor.execute(new PivotTableQueryDto(query, rows, columns));

    Assertions.assertThat(result.table).containsExactly(
            List.of(GRAND_TOTAL, GRAND_TOTAL, 22d),
            List.of(GRAND_TOTAL, "la", 13d),
            List.of(GRAND_TOTAL, "london", 9d),

            List.of("extra", GRAND_TOTAL, 9d),
            List.of("extra", "la", 4d),
            List.of("extra", "london", 5d),

            List.of("minimum expenditure", GRAND_TOTAL, 13d),
            List.of("minimum expenditure", "la", 9d),
            List.of("minimum expenditure", "london", 4d)
    );

    Assertions.assertThat(result.pivotTableCells).containsExactly(
            List.of("spending category", GRAND_TOTAL, "extra", "minimum expenditure"),
            List.of("city", "amount", "amount", "amount"),
            List.of(GRAND_TOTAL, 22d, 9d, 13d),
            List.of("la", 13d, 4d, 9d),
            List.of("london", 9d, 5d, 4d)
    );
  }

  @Test
  void testComplexPivotTableSingleMeasure(TestInfo testInfo) {
    Measure amount = Functions.sum("amount", "amount");

    List<Measure> measures = List.of(amount);
    QueryDto query = Query
            .from(this.storeName)
            .where(all(
                    criterion("city", in("paris", "lyon", "london")),
                    criterion("country", in("france", "uk"))
            )) // to reduce size of the output
            .select(List.of("spending category", "spending subcategory", "country", "city"), measures)
            .build();
    List<String> rows = List.of("country", "city");
    List<String> columns = List.of("spending category", "spending subcategory");
    verifyResults(testInfo, query, rows, columns);
  }

  @Test
  void testComplexPivotTableTwoMeasures(TestInfo testInfo) {
    Measure amount = Functions.sum("sum", "amount");
    Measure min = Functions.min("min", "amount");

    List<Measure> measures = List.of(amount, min);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("spending category", "spending subcategory", "continent", "country", "city"), measures)
            .build();
    List<String> rows = List.of("continent", "country", "city");
    List<String> columns = List.of("spending category", "spending subcategory");
    verifyResults(testInfo, query, rows, columns);
  }

  @Test
  void testGroupingOneColumnEachAxis() {
    BucketColumnSetDto bucketCS = new BucketColumnSetDto("group", "country")
            .withNewBucket("european", List.of("uk", "france"))
            .withNewBucket("anglophone", List.of("usa", "uk"))
            .withNewBucket("all", List.of("usa", "uk", "france"));
    Measure amount = Functions.sum("amount", "amount");
    ComparisonMeasureReferencePosition amountComp = new ComparisonMeasureReferencePosition(
            "amountComp",
            ABSOLUTE_DIFFERENCE,
            amount,
            Map.of("country", "c-1", "group", "g"),
            ColumnSetKey.BUCKET);

    List<Measure> measures = List.of(amountComp);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(), List.of(bucketCS), measures)
            .build();
    PivotTable result = this.executor.execute(new PivotTableQueryDto(query, List.of("country"), List.of("group")));
    Assertions.assertThat(result.table).containsExactly(
            List.of("european", "uk", 0d),
            List.of("european", "france", -1d),
            List.of("anglophone", "usa", 0d),
            List.of("anglophone", "uk", -30d),
            List.of("all", "usa", 0d),
            List.of("all", "uk", -30d),
            List.of("all", "france", -1d)
    );

    Assertions.assertThat(result.pivotTableCells).containsExactly(
            Arrays.asList("group", "european", "anglophone", "all"),
            Arrays.asList("country", "amountComp", "amountComp", "amountComp"),
            Arrays.asList("uk", 0d, -30d, -30d),
            Arrays.asList("france", -1d, null, -1d),
            Arrays.asList("usa", null, 0d, 0d)
    );
  }

  @Test
  void testGroupingMultipleColumns(TestInfo testInfo) {
    BucketColumnSetDto bucketCS = new BucketColumnSetDto("group", "country")
            .withNewBucket("european", List.of("uk", "france"))
            .withNewBucket("anglophone", List.of("usa", "uk"))
            .withNewBucket("all", List.of("usa", "uk", "france"));
    Measure amount = Functions.sum("amount", "amount");
    ComparisonMeasureReferencePosition amountComp = new ComparisonMeasureReferencePosition(
            "amountComp",
            ABSOLUTE_DIFFERENCE,
            amount,
            Map.of("country", "c-1", "group", "g"),
            ColumnSetKey.BUCKET);

    List<Measure> measures = List.of(amountComp);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("spending category"), List.of(bucketCS), measures)
            .build();
    verifyResults(testInfo, query, List.of("group", "country"), List.of("spending category"));
  }

  @Test
  void testComplexPivotTableParentComparisonMeasure(TestInfo testInfo) {
    Measure amount = Functions.sum("amount", "amount");
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent",
            ABSOLUTE_DIFFERENCE, // use this method instead of DIVIDE to have round figures
            amount,
            List.of("city", "country", "continent"));

    List<Measure> measures = List.of(amount, pOp);
    QueryDto query = Query
            .from(this.storeName)
            .where(all(
                    criterion("city", in("paris", "lyon", "london")),
                    criterion("country", in("france", "uk"))
            )) // to reduce size of the output
            .select(List.of("spending category", "spending subcategory", "continent", "country", "city"), measures)
            .build();
    List<String> rows = List.of("continent", "country", "city");
    List<String> columns = List.of("spending category", "spending subcategory");
    verifyResults(testInfo, query, rows, columns);
  }

  @Test
  void testIncorrectQueryRollup() {
    Measure amount = Functions.sum("amount", "amount");
    List<Measure> measures = List.of(amount);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("spending category", "spending subcategory", "continent", "country", "city"), measures)
            .rollup("spending category") // rollup is not supported with the pivot table API
            .build();
    List<String> rows = List.of("continent", "country", "city");
    List<String> columns = List.of("spending category", "spending subcategory");
    Assertions.assertThatThrownBy(() -> this.executor.execute(new PivotTableQueryDto(query, rows, columns)))
            .hasMessage("Rollup is not supported by this API");
  }

  @Test
  void testMissingFieldOnAxis() {
    Measure amount = Functions.sum("amount", "amount");
    List<Measure> measures = List.of(amount);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("spending category", "spending subcategory", "continent", "country", "city"), measures)
            .build();
    List<String> rowsWithoutContinent = List.of("country", "city");
    List<String> columns = List.of("spending category", "spending subcategory");
    // continent is missing despite the fact it is in the select
    Assertions.assertThatThrownBy(() -> this.executor.execute(new PivotTableQueryDto(query, rowsWithoutContinent, columns)))
            .hasMessage("[continent] in select but not on rows or columns. Please add those fields on one axis");

    List<String> rows = List.of("continent", "country", "city");
    List<String> columnsWithoutContinent = List.of("spending category");
    // spending subcategory is missing despite the fact it is in the select
    Assertions.assertThatThrownBy(() -> this.executor.execute(new PivotTableQueryDto(query, rows, columnsWithoutContinent)))
            .hasMessage("[spending subcategory] in select but not on rows or columns. Please add those fields on one axis");
  }

  @Test
  void testUnknownFieldOnAxis() {
    Measure amount = Functions.sum("amount", "amount");
    List<Measure> measures = List.of(amount);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("spending category", "spending subcategory", "continent", "country", "city"), measures)
            .build();
    List<String> rows = List.of("unknown", "continent", "country", "city");
    List<String> columns = List.of("spending category", "spending subcategory");
    // continent is missing despite the fact it is in the select
    Assertions.assertThatThrownBy(() -> this.executor.execute(new PivotTableQueryDto(query, rows, columns)))
            .hasMessage("[unknown] on rows or columns by not in select. Please add those fields in select");
  }

  @Test
  void testPivotTableWithRollupsAndUnionDistincts() {
    // No need to run for the other DB.
    Assumptions.assumeTrue(this.queryEngine.getClass().getName().contains(TestClass.Type.DUCKDB.className));

    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("spending category", "spending subcategory", "continent", "country", "city"), List.of(CountMeasure.INSTANCE))
            .build();
    List<String> rows = List.of("continent", "country", "city");
    List<String> columns = List.of("spending category", "spending subcategory");
    PivotTable expectedPivotTable = this.executor.execute(new PivotTableQueryDto(query, rows, columns));

    String base = "select continent, country, city, \"spending category\", \"spending subcategory\", count(*) as \"_contributors_count_\" from \"" + this.storeName + "\" group by";

    // This is the kind of query that BigQueryEngine generates because it does not support grouping sets for the time being.
    // Once it is supported, this test can be removed.
    String sql = String.format(
            "%1$s rollup(continent, country, city, \"spending category\", \"spending subcategory\") " +
                    "union distinct " +
                    "%1$s rollup(\"spending category\", \"spending subcategory\", continent, country, city) " +
                    "union distinct " +
                    "%1$s rollup(continent, \"spending category\", \"spending subcategory\", country, city) " +
                    "union distinct " +
                    "%1$s rollup(continent, country, \"spending category\", \"spending subcategory\", city)", base);
    RowTable actualTable = (RowTable) this.executor.execute(sql);
    ColumnarTable actualColumnarTable = TestUtil.convert(actualTable, Set.of(CountMeasure.INSTANCE));

    actualColumnarTable = TableUtils.selectAndOrderColumns(actualColumnarTable,
            expectedPivotTable.table.headers().stream().filter(h -> !h.isMeasure()).map(Header::name).toList(),
            List.of(CountMeasure.INSTANCE));
    actualColumnarTable = (ColumnarTable) TableUtils.orderRows(actualColumnarTable);

    for (int rowIndex = 0; rowIndex < expectedPivotTable.table.count(); rowIndex++) {
      for (int i = 0; i < expectedPivotTable.table.headers().size(); i++) {
        Object cell = expectedPivotTable.table.getColumn(i).get(rowIndex);
        if (cell.equals(QueryEngine.TOTAL) || cell.equals(QueryEngine.GRAND_TOTAL)) {
          expectedPivotTable.table.getColumn(i).set(rowIndex, null);
        }
      }
    }
    Table expectedTable = TableUtils.orderRows((ColumnarTable) expectedPivotTable.table);
    Assertions.assertThat(actualColumnarTable).containsExactlyElementsOf(expectedTable);
  }

  private void verifyResults(TestInfo testInfo, QueryDto query, List<String> rows, List<String> columns) {
    PivotTable pt = this.executor.execute(new PivotTableQueryDto(query, rows, columns));
    Table expectedTabular = tableFromFile(testInfo);

    Assertions.assertThat(pt.table).containsExactlyElementsOf(ImmutableList.copyOf(expectedTabular.iterator()));
    Assertions.assertThat(pt.table.headers()).containsExactlyElementsOf(expectedTabular.headers());

    List<List<Object>> expectedPivotTable = pivotTableFromFile(testInfo);
    Assertions.assertThat(pt.pivotTableCells).containsExactlyElementsOf(expectedPivotTable);
  }
}
