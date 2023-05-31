package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.jackson.JacksonUtil;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.Field;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
  void test() {
    Measure amount = Functions.sum("amount", "amount");
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", ComparisonMethod.DIVIDE, amount, List.of("city", "country", "continent"));
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of("spending_category", "spending_subcategory", "continent", "country", "city"), List.of(amount))
//            .rollup(List.of("spending_category", "continent", "country", "city"))
            .build();

    this.executor.execute(query, List.of("continent", "country", "city"), List.of("spending_category", "spending_subcategory"), true);

    Table result = this.executor.execute("select \"country\", \"spending_category\", \"spending_subcategory\", \"continent\", \"city\", sum(\"amount\") as \"amount\" " +
//            "from \"storetestduckdbparentcomparisonwithothercolumn\" group by cube(" + String.join(",", "continent", "country", "city", "spending_category") +
            "from " + this.storeName + " group by grouping sets(" +
            // Details most granular

            // Rows
            "(" + String.join(",", "continent", "country", "city") + ")," +
            "(" + String.join(",", "continent", "country") + ")," +
            "(" + String.join(",", "continent") + ")," +

            // Cols
            "(" + String.join(",", "spending_category", "spending_subcategory") + ")," +
            "(" + String.join(",", "spending_category") + ")," +

            // all combinations
            "(" + String.join(",", "continent", "country", "city", "spending_category", "spending_subcategory") + ")," + // all concatenate
            "(" + String.join(",", "continent", "country", "city", "spending_category") + ")," + // remove col
            "(" + String.join(",", "continent", "country", "spending_category", "spending_subcategory") + ")," + // remove row
            "(" + String.join(",", "continent", "spending_category", "spending_subcategory") + ")," + // remove row

            "(" + String.join(",", "continent", "country", "spending_category") + ")," + // remove col and row
            "(" + String.join(",", "continent", "spending_category") + ")," + // remove col and row until 1 each

            // GT
            "()" +

            ") limit 10000");

    result = this.executor.execute(query);
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

    result.show();
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
}
