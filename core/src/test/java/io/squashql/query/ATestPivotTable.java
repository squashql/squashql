package io.squashql.query;

import com.google.common.collect.ImmutableList;
import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.BucketColumnSetDto;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.PivotTableQueryDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.PivotTable;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import io.squashql.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.squashql.query.ComparisonMethod.ABSOLUTE_DIFFERENCE;
import static io.squashql.query.Functions.*;
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;

@TestClass(ignore = {TestClass.Type.SNOWFLAKE})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestPivotTable extends ABaseTestQuery {

  protected String storeSpending = "storespending" + getClass().getSimpleName().toLowerCase();
  protected String storePopulation = "storepopulation" + getClass().getSimpleName().toLowerCase();

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField city = new TableTypedField(this.storeSpending, "city", String.class);
    TableTypedField country = new TableTypedField(this.storeSpending, "country", String.class);
    TableTypedField continent = new TableTypedField(this.storeSpending, "continent", String.class);
    TableTypedField spendingCategory = new TableTypedField(this.storeSpending, "spending category", String.class);
    TableTypedField spendingSubcategory = new TableTypedField(this.storeSpending, "spending subcategory", String.class);
    TableTypedField amount = new TableTypedField(this.storeSpending, "amount", double.class);
    TableTypedField population = new TableTypedField(this.storePopulation, "population", double.class);
    return Map.of(
            this.storeSpending, List.of(city, country, continent, spendingCategory, spendingSubcategory, amount),
            this.storePopulation, List.of(country, continent, population));
  }

  @Override
  protected void loadData() {
    this.tm.load(this.storeSpending, List.of(
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


    this.tm.load(this.storePopulation, List.of(
            new Object[]{"france", "eu", 70d},
            new Object[]{"uk", "eu", 65d},
            new Object[]{"usa", "am", 330d}
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
    final List<Field> fields = tableFields(List.of("continent", "country", "city"));
    QueryDto queryWithoutRollup = Query
            .from(this.storeSpending)
            .select(fields, List.of(amount))
            .build();
    QueryDto queryRollup = Query
            .from(this.storeSpending)
            .select(fields, List.of(amount))
            .rollup(fields)
            .build();
    Table resultRollup = this.executor.executeQuery(queryRollup);

    // NO COLUMNS
    {
      PivotTable result = this.executor.executePivotQuery(new PivotTableQueryDto(queryWithoutRollup, fields, List.of()));
      Assertions.assertThat(result.table).containsExactlyInAnyOrderElementsOf(resultRollup);
    }
    // NO ROWS
    {
      PivotTable result = this.executor.executePivotQuery(new PivotTableQueryDto(queryWithoutRollup, List.of(), fields));
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
            .from(this.storeSpending)
            .where(criterion("city", in("la", "london"))) // to reduce size of the output
            .select(tableFields(List.of("spending category", "city")), List.of(amount))
            .build();
    List<Field> rows = tableFields(List.of("city"));
    List<Field> columns = tableFields(List.of("spending category"));
    PivotTable result = this.executor.executePivotQuery(new PivotTableQueryDto(query, rows, columns));

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
            .from(this.storeSpending)
            .where(all(
                    criterion("city", in("paris", "lyon", "london")),
                    criterion("country", in("france", "uk"))
            )) // to reduce size of the output
            .select(tableFields(List.of("spending category", "spending subcategory", "country", "city")), measures)
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
            .from(this.storeSpending)
            .select(tableFields(List.of("spending category", "spending subcategory", "continent", "country", "city")), measures)
            .build();
    List<String> rows = List.of("continent", "country", "city");
    List<String> columns = List.of("spending category", "spending subcategory");
    verifyResults(testInfo, query, rows, columns);
  }

  @Test
  void testGroupingOneColumnEachAxis() {
    final Field group = tableField("group");
    final Field country = tableField("country");
    BucketColumnSetDto bucketCS = new BucketColumnSetDto("group", country)
            .withNewBucket("european", List.of("uk", "france"))
            .withNewBucket("anglophone", List.of("usa", "uk"))
            .withNewBucket("all", List.of("usa", "uk", "france"));
    Measure amount = Functions.sum("amount", "amount");
    ComparisonMeasureReferencePosition amountComp = new ComparisonMeasureReferencePosition(
            "amountComp",
            ABSOLUTE_DIFFERENCE,
            amount,
            Map.of(bucketCS.field, "c-1", tableField("group"), "g"),
            ColumnSetKey.BUCKET);

    List<Measure> measures = List.of(amountComp);
    QueryDto query = Query
            .from(this.storeSpending)
            .select(List.of(), List.of(bucketCS), measures)
            .build();
    PivotTable result = this.executor.executePivotQuery(new PivotTableQueryDto(query, Collections.singletonList(country),
            Collections.singletonList(group)));
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
    final Field group = tableField("group");
    final Field country = tableField("country");
    BucketColumnSetDto bucketCS = new BucketColumnSetDto("group", country)
            .withNewBucket("european", List.of("uk", "france"))
            .withNewBucket("anglophone", List.of("usa", "uk"))
            .withNewBucket("all", List.of("usa", "uk", "france"));
    Measure amount = Functions.sum("amount", "amount");
    ComparisonMeasureReferencePosition amountComp = new ComparisonMeasureReferencePosition(
            "amountComp",
            ABSOLUTE_DIFFERENCE,
            amount,
            Map.of(country, "c-1", group, "g"),
            ColumnSetKey.BUCKET);

    List<Measure> measures = List.of(amountComp);
    QueryDto query = Query
            .from(this.storeSpending)
            .select(tableFields(List.of("spending category")), List.of(bucketCS), measures)
            .build();
    verifyResults(testInfo, query, List.of("group", "country"), List.of("spending category"));
  }

  @Test
  void testComplexPivotTableParentComparisonMeasure(TestInfo testInfo) {
    Measure amount = Functions.sum("amount", "amount");
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent",
            ABSOLUTE_DIFFERENCE, // use this method instead of DIVIDE to have round figures
            amount,
            tableFields(List.of("continent", "country", "city")));

    List<Measure> measures = List.of(amount, pOp);
    QueryDto query = Query
            .from(this.storeSpending)
            .where(all(
                    criterion("city", in("paris", "lyon", "london")),
                    criterion("country", in("france", "uk"))
            )) // to reduce size of the output
            .select(tableFields(List.of("spending category", "spending subcategory", "continent", "country", "city")), measures)
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
            .from(this.storeSpending)
            .select(tableFields(List.of("spending category", "spending subcategory", "continent", "country", "city")), measures)
            .rollup(tableField("spending category")) // rollup is not supported with the pivot table API
            .build();
    List<Field> rows = tableFields(List.of("continent", "country", "city"));
    List<Field> columns = tableFields(List.of("spending category", "spending subcategory"));
    Assertions.assertThatThrownBy(() -> this.executor.executePivotQuery(new PivotTableQueryDto(query, rows, columns)))
            .hasMessage("Rollup is not supported by this API");
  }

  @Test
  void testMissingFieldOnAxis() {
    Measure amount = Functions.sum("amount", "amount");
    List<Measure> measures = List.of(amount);
    QueryDto query = Query
            .from(this.storeSpending)
            .select(tableFields(List.of("spending category", "spending subcategory", "continent", "country", "city")), measures)
            .build();
    List<Field> rowsWithoutContinent = tableFields(List.of("country", "city"));
    List<Field> columns = tableFields(List.of("spending category", "spending subcategory"));
    // continent is missing despite the fact it is in the select
    Assertions.assertThatThrownBy(() -> this.executor.executePivotQuery(new PivotTableQueryDto(query, rowsWithoutContinent, columns)))
            .hasMessage("[continent] in select but not on rows or columns. Please add those fields on one axis");

    List<Field> rows = tableFields(List.of("continent", "country", "city"));
    List<Field> columnsWithoutContinent = tableFields(List.of("spending category"));
    // spending subcategory is missing despite the fact it is in the select
    Assertions.assertThatThrownBy(() -> this.executor.executePivotQuery(new PivotTableQueryDto(query, rows, columnsWithoutContinent)))
            .hasMessage("[spending subcategory] in select but not on rows or columns. Please add those fields on one axis");
  }

  @Test
  void testUnknownFieldOnAxis() {
    Measure amount = Functions.sum("amount", "amount");
    List<Measure> measures = List.of(amount);
    QueryDto query = Query
            .from(this.storeSpending)
            .select(tableFields(List.of("spending category", "spending subcategory", "continent", "country", "city")), measures)
            .build();
    List<Field> rows = tableFields(List.of("unknown", "continent", "country", "city"));
    List<Field> columns = tableFields(List.of("spending category", "spending subcategory"));
    // continent is missing despite the fact it is in the select
    Assertions.assertThatThrownBy(() -> this.executor.executePivotQuery(new PivotTableQueryDto(query, rows, columns)))
            .hasMessage("[unknown] on rows or columns by not in select. Please add those fields in select");
  }

  @Test
  void testDrillingAcross(TestInfo testInfo) {
    Measure amount = Functions.sum("amount", "amount");
    Measure pop = Functions.sum("population", "population");

    List<Measure> measuresSpending = List.of(amount);
    QueryDto query1 = Query
            .from(this.storeSpending)
            .select(tableFields(List.of("spending category", "continent", "country")), measuresSpending)
            .build();

    List<Measure> measuresPop = List.of(pop);
    QueryDto query2 = Query
            .from(this.storePopulation)
            .select(tableFields(List.of("continent", "country")), measuresPop)
            .build();

    verifyResults(testInfo, query1, query2, JoinType.LEFT, List.of("continent", "country"), List.of("spending category"));
  }

  @Test
  void testDrillingAcrossAndColumnSet(TestInfo testInfo) {
    // TODO write a test with the first query containing a columnset.
    Measure amount = Functions.sum("amount", "amount");
    Measure pop = Functions.sum("population", "population");

    BucketColumnSetDto bucketCS = new BucketColumnSetDto("group", tableField("spending category"))
            .withNewBucket("group1", List.of("extra"))
            .withNewBucket("group2", List.of("extra", "minimum expenditure"));

    List<Measure> measuresSpending = List.of(amount);
    QueryDto query1 = Query
            .from(this.storeSpending)
            .select(tableFields(List.of("country")), List.of(bucketCS), measuresSpending)
            .build();

    executor.executeQuery(query1).show();
    PivotTableQueryDto pivotTableQueryDto = new PivotTableQueryDto(query1, tableFields(List.of("group", "spending category")), List.of(tableField("country")));
    executor.executePivotQuery(pivotTableQueryDto).show();
    /*
    +---------+---------------------+-------------+--------+--------+--------+
    | country |             country | Grand Total | france |     uk |    usa |
    |   group |   spending category |      amount | amount | amount | amount |
    +---------+---------------------+-------------+--------+--------+--------+
    |  group1 |               extra |        17.0 |    2.0 |    5.0 |   10.0 |
    |  group2 |               extra |        17.0 |    2.0 |    5.0 |   10.0 |
    |  group2 | minimum expenditure |        39.0 |    6.0 |    4.0 |   29.0 |
    +---------+---------------------+-------------+--------+--------+--------+
     */
    List<Measure> measuresPop = List.of(pop);
    QueryDto query2 = Query
            .from(this.storePopulation)
            .select(tableFields(List.of("country")), measuresPop)
            .build();
    pivotTableQueryDto = new PivotTableQueryDto(query2, List.of(), List.of(tableField("country")));
    executor.executePivotQuery(pivotTableQueryDto).show();
    /*
    +-------------+------------+------------+------------+
    | Grand Total |     france |         uk |        usa |
    |  population | population | population | population |
    +-------------+------------+------------+------------+
    |       465.0 |       70.0 |       65.0 |      330.0 |
    +-------------+------------+------------+------------+
     */

    executor.executePivotQueryMerge(query1, query2, tableFields(List.of("group", "spending category")), List.of(tableField("country")), JoinType.INNER, null)
            .show();
    /*
    +-------------+---------------------+-------------+-------------+--------+------------+--------+------------+--------+------------+
    |     country |             country | Grand Total | Grand Total | france |     france |     uk |         uk |    usa |        usa |
    |       group |   spending category |      amount |  population | amount | population | amount | population | amount | population |
    +-------------+---------------------+-------------+-------------+--------+------------+--------+------------+--------+------------+
    |      group1 |               extra |        17.0 |        null |    2.0 |       null |    5.0 |       null |   10.0 |       null |
    |      group2 |               extra |        17.0 |        null |    2.0 |       null |    5.0 |       null |   10.0 |       null |
    |      group2 | minimum expenditure |        39.0 |        null |    6.0 |       null |    4.0 |       null |   29.0 |       null |
    | Grand Total |         Grand Total |        null |       465.0 |   null |       70.0 |   null |       65.0 |   null |      330.0 |
    +-------------+---------------------+-------------+-------------+--------+------------+--------+------------+--------+------------+
     */
    // TODO write the results in files and add the check below.
    Assertions.fail("to do");
  }

  private void verifyResults(TestInfo testInfo, QueryDto query, List<String> rows, List<String> columns) {
    verifyResults(testInfo, query, null, null, rows, columns);
  }

  /**
   * To save in file '*.tabular.json': System.out.println(TestUtil.tableToJson(pivotTable.table));
   * To save in file '*.pivottable.json': System.out.println(JacksonUtil.serialize(pivotTable.pivotTableCells));
   */
  private void verifyResults(TestInfo testInfo, QueryDto query1, QueryDto query2, JoinType joinType, List<String> rows, List<String> columns) {
    PivotTable pt = query2 == null
            ? this.executor.executePivotQuery(new PivotTableQueryDto(query1, tableFields(rows), tableFields(columns)))
            : this.executor.executePivotQueryMerge(query1, query2, tableFields(rows), tableFields(columns), joinType, null);
    Table expectedTabular = tableFromFile(testInfo);

    Assertions.assertThat(pt.table).containsExactlyElementsOf(ImmutableList.copyOf(expectedTabular.iterator()));
    Assertions.assertThat(pt.table.headers()).containsExactlyElementsOf(expectedTabular.headers());

    List<List<Object>> expectedPivotTable = pivotTableFromFile(testInfo);
    Assertions.assertThat(pt.pivotTableCells).containsExactlyElementsOf(expectedPivotTable);
  }
}
