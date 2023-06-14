package io.squashql.query;

import com.google.common.collect.ImmutableList;
import io.squashql.TestClass;
import io.squashql.jackson.JacksonUtil;
import io.squashql.query.builder.CanAddRollup;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.BucketColumnSetDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.SimpleTableDto;
import io.squashql.store.Field;
import io.squashql.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Supplier;

import static io.squashql.query.ComparisonMethod.ABSOLUTE_DIFFERENCE;
import static io.squashql.query.Functions.*;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;

@TestClass(ignore = {TestClass.Type.BIGQUERY, TestClass.Type.SNOWFLAKE})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestPivotTable extends ABaseTestQuery {

  protected String storeName = "store" + getClass().getSimpleName().toLowerCase();

  @Override
  protected Map<String, List<Field>> getFieldsByStore() {
    Field city = new Field(this.storeName, "city", String.class);
    Field country = new Field(this.storeName, "country", String.class);
    Field continent = new Field(this.storeName, "continent", String.class);
    Field spendingCategory = new Field(this.storeName, "spending category", String.class);
    Field spendingSubcategory = new Field(this.storeName, "spending subcategory", String.class);
    Field amount = new Field(this.storeName, "amount", double.class);
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

    CanAddRollup base = Query
            .from(this.storeName)
            .select(List.of("continent", "country", "city"), List.of(amount));
    QueryDto queryRollup = base
            .rollup(List.of("continent", "country", "city"))
            .build();
    Table resultRollup = this.executor.execute(queryRollup);

    // NO COLUMNS
    {
      Table result = this.executor.execute(base.build(), List.of("continent", "country", "city"), List.of(), true);
      Assertions.assertThat(result).containsExactlyInAnyOrderElementsOf(resultRollup);
    }
    // NO ROWS
    {
      Table result = this.executor.execute(base.build(), List.of(), List.of("continent", "country", "city"), true);
      Assertions.assertThat(result).containsExactlyInAnyOrderElementsOf(resultRollup);
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
    Table result = this.executor.execute(query, rows, columns, true);

    Assertions.assertThat(result).containsExactly(
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

    List<List<Object>> pivotTableRows = pivot(result, rows, columns, List.of("amount"));
    Assertions.assertThat(pivotTableRows).containsExactly(
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
    Measure amount = Functions.sum("amount", "amount");
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
    Table result = this.executor.execute(query, List.of("country"), List.of("group"), true);
    Assertions.assertThat(result).containsExactly(
            List.of("european", "uk", 0d),
            List.of("european", "france", -1d),
            List.of("anglophone", "usa", 0d),
            List.of("anglophone", "uk", -30d),
            List.of("all", "usa", 0d),
            List.of("all", "uk", -30d),
            List.of("all", "france", -1d)
    );

    List<List<Object>> pivotTableRows = pivot(result, List.of("country"), List.of("group"), List.of(amountComp.alias));
    Assertions.assertThat(pivotTableRows).containsExactly(
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

  private void verifyResults(TestInfo testInfo, QueryDto queryDto, List<String> rows, List<String> columns) {
    Table table = this.executor.execute(queryDto, rows, columns, true);
    System.out.println(TestUtil.tableToJson(table));
    Table expectedTabular = tableFromFile(testInfo);

    Assertions.assertThat(table).containsExactlyElementsOf(ImmutableList.copyOf(expectedTabular.iterator()));
    Assertions.assertThat(table.headers()).containsExactlyElementsOf(expectedTabular.headers());

    List<String> values = queryDto.measures.stream().map(Measure::alias).toList();
    List<List<Object>> pivotTableRows = pivot(table, rows, columns, values);
    System.out.println(JacksonUtil.serialize(pivotTableRows));
    toJson(table, rows, columns, values);

    List<List<Object>> expectedPivotTable = pivotTableFromFile(testInfo);
    Assertions.assertThat(pivotTableRows).containsExactlyElementsOf(expectedPivotTable);
  }

  @Test
  void testToDelete() {
    Measure amount = Functions.sum("amount", "amount");
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", ComparisonMethod.DIVIDE, amount, List.of("city", "country", "continent"));
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("spending category", "spending subcategory", "continent", "country", "city"), List.of(amount))
//            .rollup(List.of("spending category", "continent", "country", "city"))
            .build();

    List<String> rows = List.of("continent", "country", "city");
    List<String> values = List.of("spending category", "spending subcategory");
    Table result = this.executor.execute(query, rows, values, true);
    result.show();

//    this.executor.execute("select \"country\", \"spending category\", \"spending subcategory\", \"continent\", \"city\", sum(\"amount\") as \"amount\" " +
////            "from \"storetestduckdbparentcomparisonwithothercolumn\" group by cube(" + String.join(",", "continent", "country", "city", "spending category") +
//            "from " + this.storeName + " group by grouping sets(" +
//            // Details most granular
//
//            // Rows = List.of("continent", "country", "city")
//            // Total on rows
//            "(" + String.join(",", "continent", "country", "city") + ")," +
//            "(" + String.join(",", "continent", "country") + ")," +
//            "(" + String.join(",", "continent") + ")," +
//
//            // Cols = List.of("spending category", "spending subcategory")
//            "(" + String.join(",", "spending category", "spending subcategory") + ")," +
//            "(" + String.join(",", "spending category") + ")," +
//
//            // all combinations
//            "(" + String.join(",", "continent", "country", "city", "spending category", "spending subcategory") + ")," + // all concatenate
//            "(" + String.join(",", "continent", "country", "city", "spending category") + ")," + // remove col
//            "(" + String.join(",", "continent", "country", "spending category", "spending subcategory") + ")," + // remove row
//            "(" + String.join(",", "continent", "spending category", "spending subcategory") + ")," + // remove row
//
//            "(" + String.join(",", "continent", "country", "spending category") + ")," + // remove col and row
//            "(" + String.join(",", "continent", "spending category") + ")," + // remove col and row until 1 each
//
//            // GT
//            "()" +
//
//            ") limit 10000");

//    result = this.executor.execute(query);
//    Assertions.assertThat(result).containsExactly(
//            Arrays.asList("car", "eu", "france", "lyon", 0.1d, 0.1d / (0.1d + 1d)),
//            Arrays.asList("car", "eu", "france", "paris", 1d, 1d / (0.1d + 1d)),
//            Arrays.asList("car", "eu", "uk", "london", 2d, 1d),
//            Arrays.asList("hobbies", "eu", "france", "lyon", 1d, 1d / (1 + 1)),
//            Arrays.asList("hobbies", "eu", "france", "paris", 1d, 1d / (1 + 1)),
//            Arrays.asList("hobbies", "eu", "uk", "london", 5d, 1d),
//            Arrays.asList("home", "eu", "france", "lyon", 2d, 2d / (2 + 2)),
//            Arrays.asList("home", "eu", "france", "paris", 2d, 2d / (2 + 2)),
//            Arrays.asList("home", "eu", "uk", "london", 2d, 1d));

    toJson(result, null, null, null);
  }

  private static void toJson(Table result, List<String> rows, List<String> columns, List<String> values) {
    List<String> list = result.headers().stream().map(Header::name).toList();
//    Map<String, Object>[] m = new Map[(int) result.count()];
//    AtomicInteger index = new AtomicInteger();
//    result.forEach(r -> {
//      Map<String, Object> mm = (m[index.getAndIncrement()] = new HashMap<>(r.size()));
//      for (int i = 0; i < r.size(); i++) {
//        mm.put(list.get(i), r.get(i));
//      }
//    });
//    String serialize = JacksonUtil.serialize(m);
//    System.out.println(serialize);

    SimpleTableDto simpleTable = SimpleTableDto.builder()
            .rows(ImmutableList.copyOf(result.iterator()))
            .columns(list)
            .build();

    Map<String, Object> data = Map.of("rows", rows, "columns", columns, "values", values, "table", simpleTable);
    String encodedString = Base64.getEncoder().encodeToString(JacksonUtil.serialize(data).getBytes(StandardCharsets.UTF_8));
    System.out.println("http://localhost:3000?data=" + encodedString);
  }

  public static List<List<Object>> pivot(Table table, List<String> rows, List<String> columns, List<String> values) {
    Set<ObjectArrayKey> columnHeaderValues = getHeaderValues(table, columns);

    List<List<Object>> headerColumns = new ArrayList<>(); // The header values for the columns
    int size = columnHeaderValues.size() * values.size();
    // Prepare the lists
    columns.forEach(__ -> {
      List<Object> r = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        r.add(null);
      }
      headerColumns.add(r);
    });

    // Fill the lists
    int columnIndex = 0;
    for (ObjectArrayKey columnHeaderValue : columnHeaderValues) {
      for (int __ = 0; __ < values.size(); __++) {
        for (int rowIndex = 0; rowIndex < columnHeaderValue.a.length; rowIndex++) {
          headerColumns.get(rowIndex).set(columnIndex, columnHeaderValue.a[rowIndex]);
        }
        columnIndex++;
      }
    }

    Set<ObjectArrayKey> rowHeaderValues = getHeaderValues(table, rows);

    int[] rowMapping = getMapping(table, rows);
    int[] colMapping = getMapping(table, columns);
    List<List<Object>> cells = new ArrayList<>(rowHeaderValues.size() * size); // The values of the cells.
    rowHeaderValues.forEach(rowPoint -> {
      Object[] buffer = new Object[rows.size() + columns.size()];
      List<Object> r = new ArrayList<>();
      cells.add(r);
      for (int i = 0; i < rowMapping.length; i++) {
        buffer[rowMapping[i]] = rowPoint.a[i];
      }

      for (int i = 0; i < columnHeaderValues.size(); i++) {
        for (int j = 0; j < colMapping.length; j++) {
          buffer[colMapping[j]] = headerColumns.get(j).get(i * values.size());
        }
        int position = ((ColumnarTable) table).pointDictionary.get().getPosition(buffer);

        for (String value : values) {
          r.add(position >= 0 ? table.getColumnValues(value).get(position) : null);
        }
      }
    });

    List<List<Object>> finalRows = new ArrayList<>();
    Supplier<List<Object>> listSpwaner = () -> {
      finalRows.add(new ArrayList<>(rows.size() + size));
      return finalRows.get(finalRows.size() - 1);
    };
    for (int i = 0; i < columns.size(); i++) {
      List<Object> r = listSpwaner.get();
      for (int j = 0; j < rows.size(); j++) {
        r.add(columns.get(i)); // recopy name of the column
      }
      r.addAll(headerColumns.get(i));
    }

    List<Object> r = listSpwaner.get();
    r.addAll(rows);
    for (int i = 0; i < columnHeaderValues.size(); i++) {
      for (String value : values) {
        r.add(value);
      }
    }

    int[] index = new int[1];
    rowHeaderValues.forEach(a -> {
      List<Object> rr = listSpwaner.get();
      rr.addAll(Arrays.asList(a.a));
      rr.addAll(cells.get(index[0]++));
    });

    System.out.println(TableUtils.toString(finalRows, String::valueOf, line -> line.equals(columns.size())));
    return finalRows;
  }

  private static Set<ObjectArrayKey> getHeaderValues(Table table, List<String> columns) {
    int[] mapping = getMapping(table, columns);

    LinkedHashSet<ObjectArrayKey> result = new LinkedHashSet<>();
    table.forEach(row -> {
      Object[] columnValues = new Object[columns.size()];
      for (int i = 0; i < columns.size(); i++) {
        columnValues[i] = row.get(mapping[i]);
      }
      result.add(new ObjectArrayKey(columnValues));
    });
    return result;
  }

  private static int[] getMapping(Table table, List<String> columns) {
    int[] mapping = new int[columns.size()];
    Arrays.fill(mapping, -1);
    for (int i = 0; i < columns.size(); i++) {
      for (int j = 0; j < table.headers().size(); j++) {
        if (table.headers().get(j).name().equals(columns.get(i))) {
          mapping[i] = j;
          break;
        }
      }
    }
    return mapping;
  }

  private static class ObjectArrayKey {
    final Object[] a;

    private ObjectArrayKey(Object[] a) {
      this.a = a;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ObjectArrayKey that = (ObjectArrayKey) o;
      return Arrays.equals(this.a, that.a);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(this.a);
    }

    @Override
    public String toString() {
      return Arrays.toString(this.a);
    }
  }
}
