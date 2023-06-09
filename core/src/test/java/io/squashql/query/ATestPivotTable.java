package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.jackson.JacksonUtil;
import io.squashql.query.builder.CanAddRollup;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@TestClass(ignore = {TestClass.Type.BIGQUERY, TestClass.Type.SNOWFLAKE})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestPivotTable extends ABaseTestQuery {

  protected String storeName = "store" + getClass().getSimpleName().toLowerCase();

  @Override
  protected Map<String, List<Field>> getFieldsByStore() {
    Field city = new Field(this.storeName, "city", String.class);
    Field country = new Field(this.storeName, "country", String.class);
    Field continent = new Field(this.storeName, "continent", String.class);
    Field spendingCategory = new Field(this.storeName, "spending_category", String.class);
    Field spendingSubcategory = new Field(this.storeName, "spending_subcategory", String.class);
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

    {
      Table result = this.executor.execute(base.build(), List.of("continent", "country", "city"), List.of(), true);
      Assertions.assertThat(result).containsExactlyInAnyOrderElementsOf(resultRollup);
    }

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

    CanAddRollup base = Query
            .from(this.storeName)
            .select(List.of("spending_category", "city"), List.of(amount));
    List<String> rows = List.of("city");
    List<String> columns = List.of("spending_category");
    Table result = this.executor.execute(base.build(), rows, columns, true);

    result.show();
    toJson(result);
//    pivot(result, rows, columns, List.of("amount"));
//    QueryDto queryRollup = base
//            .rollup(List.of("continent", "country", "city"))
//            .build();
//    Table resultRollup = this.executor.execute(queryRollup);
//    Assertions.assertThat(result).containsExactlyInAnyOrderElementsOf(resultRollup);
  }

  @Test
  void testDrawPT() {
    Measure amount = Functions.sum("amount", "amount");
    Measure mean = Functions.avg("mean", "amount");

    List<Measure> measures = List.of(amount, mean);
    CanAddRollup base = Query
            .from(this.storeName)
            .select(List.of("spending_category", "spending_subcategory", "country", "city"), measures);
    List<String> rows = List.of("country", "city");
    List<String> columns = List.of("spending_category", "spending_subcategory");
    Table result = this.executor.execute(base.build(), rows, columns, true);

    result.show();
    toJson(result);
    pivot(result, rows, columns, measures.stream().map(Measure::alias).toList());
//    QueryDto queryRollup = base
//            .rollup(List.of("continent", "country", "city"))
//            .build();
//    Table resultRollup = this.executor.execute(queryRollup);
//    Assertions.assertThat(result).containsExactlyInAnyOrderElementsOf(resultRollup);
  }

  @Test
  void test() {
    Measure amount = Functions.sum("amount", "amount");
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", ComparisonMethod.DIVIDE, amount, List.of("city", "country", "continent"));
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("spending_category", "spending_subcategory", "continent", "country", "city"), List.of(amount))
//            .rollup(List.of("spending_category", "continent", "country", "city"))
            .build();

    Table result = this.executor.execute(query, List.of("continent", "country", "city"), List.of("spending_category", "spending_subcategory"), true);
    result.show();

//    this.executor.execute("select \"country\", \"spending_category\", \"spending_subcategory\", \"continent\", \"city\", sum(\"amount\") as \"amount\" " +
////            "from \"storetestduckdbparentcomparisonwithothercolumn\" group by cube(" + String.join(",", "continent", "country", "city", "spending_category") +
//            "from " + this.storeName + " group by grouping sets(" +
//            // Details most granular
//
//            // Rows = List.of("continent", "country", "city")
//            // Total on rows
//            "(" + String.join(",", "continent", "country", "city") + ")," +
//            "(" + String.join(",", "continent", "country") + ")," +
//            "(" + String.join(",", "continent") + ")," +
//
//            // Cols = List.of("spending_category", "spending_subcategory")
//            "(" + String.join(",", "spending_category", "spending_subcategory") + ")," +
//            "(" + String.join(",", "spending_category") + ")," +
//
//            // all combinations
//            "(" + String.join(",", "continent", "country", "city", "spending_category", "spending_subcategory") + ")," + // all concatenate
//            "(" + String.join(",", "continent", "country", "city", "spending_category") + ")," + // remove col
//            "(" + String.join(",", "continent", "country", "spending_category", "spending_subcategory") + ")," + // remove row
//            "(" + String.join(",", "continent", "spending_category", "spending_subcategory") + ")," + // remove row
//
//            "(" + String.join(",", "continent", "country", "spending_category") + ")," + // remove col and row
//            "(" + String.join(",", "continent", "spending_category") + ")," + // remove col and row until 1 each
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

    toJson(result);
  }

  private static void toJson(Table result) {
    List<String> list = result.headers().stream().map(Header::name).toList();
    Map<String, Object>[] m = new Map[(int) result.count()];
    AtomicInteger index = new AtomicInteger();
    result.forEach(r -> {
      Map<String, Object> mm = (m[index.getAndIncrement()] = new HashMap<>(r.size()));
      for (int i = 0; i < r.size(); i++) {
        mm.put(list.get(i), r.get(i));
      }
    });
    System.out.println(JacksonUtil.serialize(m));
  }

  public static void pivot(Table table, List<String> rows, List<String> columns, List<String> values) {
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
          r.add(table.getColumnValues(value).get(position));
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
