package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestGrandTotalComparison extends ABaseTestQuery {

  private final String storeName = "store" + getClass().getSimpleName().toLowerCase();
  private final List<Field> fields = tableFields(List.of("continent", "country", "city")); // ancestors
  private final Measure amount = Functions.sum("amount", "amount");
  private final ComparisonMeasureGrandTotal percentOfGT = new ComparisonMeasureGrandTotal("percentOfParent", ComparisonMethod.DIVIDE, this.amount);

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField city = new TableTypedField(this.storeName, "city", String.class);
    TableTypedField country = new TableTypedField(this.storeName, "country", String.class);
    TableTypedField continent = new TableTypedField(this.storeName, "continent", String.class);
    TableTypedField spendingCategory = new TableTypedField(this.storeName, "spending_category", String.class);
    TableTypedField amount = new TableTypedField(this.storeName, "amount", double.class);
    return Map.of(this.storeName, List.of(city, country, continent, spendingCategory, amount));
  }

  @Override
  protected void loadData() {
    this.tm.load(this.storeName, List.of(
            new Object[]{"paris", "france", "eu", "car", 1d},
            new Object[]{"paris", "france", "eu", "home", 2d},
            new Object[]{"paris", "france", "eu", "hobbies", 1d},
            new Object[]{"lyon", "france", "eu", "car", 0.1d},
            new Object[]{"lyon", "france", "eu", "home", 2d},
            new Object[]{"lyon", "france", "eu", "hobbies", 1d},
            new Object[]{"london", "uk", "eu", "car", 2d},
            new Object[]{"london", "uk", "eu", "home", 2d},
            new Object[]{"london", "uk", "eu", "hobbies", 5d}
    ));
  }

  @Test
  void testSimple() {
    QueryDto query = Query
            .from(this.storeName)
            .select(this.fields, List.of(this.amount, this.percentOfGT))
            .build();

    Table result = this.executor.executeQuery(query);
    double gt = 16.1d; // amount on GrandTotal
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("eu", "france", "lyon", 3.1d, 3.1d / gt),
            Arrays.asList("eu", "france", "paris", 4d, 4d / gt),
            Arrays.asList("eu", "uk", "london", 9d, 9d / gt));

    query = Query
            .from(this.storeName)
            .select(this.fields, List.of(this.amount, this.percentOfGT))
            .rollup(this.fields)
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList(GRAND_TOTAL, GRAND_TOTAL, GRAND_TOTAL, 16.1d, 1d),
            Arrays.asList("eu", TOTAL, TOTAL, 16.1d, 1d),
            Arrays.asList("eu", "france", TOTAL, 7.1d, 7.1d / gt),
            Arrays.asList("eu", "france", "lyon", 3.1d, 3.1d / gt),
            Arrays.asList("eu", "france", "paris", 4d, 4d / gt),
            Arrays.asList("eu", "uk", TOTAL, 9d, 9d / gt),
            Arrays.asList("eu", "uk", "london", 9d, 9d / gt));
  }

  @Test
  void testCrossjoinWithOtherColumn() {
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("spending_category", "continent", "country", "city")), List.of(this.amount, this.percentOfGT))
            .build();

    Table result = this.executor.executeQuery(query);
    double gt = 16.1d;
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("car", "eu", "france", "lyon", 0.1d, 0.1d / gt),
            Arrays.asList("car", "eu", "france", "paris", 1d, 1d / gt),
            Arrays.asList("car", "eu", "uk", "london", 2d, 2d / gt),
            Arrays.asList("hobbies", "eu", "france", "lyon", 1d, 1d / gt),
            Arrays.asList("hobbies", "eu", "france", "paris", 1d, 1d / gt),
            Arrays.asList("hobbies", "eu", "uk", "london", 5d, 5d / gt),
            Arrays.asList("home", "eu", "france", "lyon", 2d, 2d / gt),
            Arrays.asList("home", "eu", "france", "paris", 2d, 2d / gt),
            Arrays.asList("home", "eu", "uk", "london", 2d, 2d / gt));
  }

  @Test
  void testCrossjoinWithOtherColumnAndFilters() {
    // Filter on other category
    QueryDto query = Query
            .from(this.storeName)
            .where(Functions.criterion(tableField("spending_category"), Functions.eq("car")))
            .select(tableFields(List.of("spending_category", "continent", "country", "city")), List.of(this.amount, this.percentOfGT))
            .build();

    Table result = this.executor.executeQuery(query);
    double gt = 3.1d;
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("car", "eu", "france", "lyon", 0.1d, 0.1d / gt),
            Arrays.asList("car", "eu", "france", "paris", 1d, 1d / gt),
            Arrays.asList("car", "eu", "uk", "london", 2d, 2d / gt));

    query = Query
            .from(this.storeName)
            .where(Functions.criterion(tableField("country"), Functions.eq("france")))
            .select(tableFields(List.of("spending_category", "continent", "country", "city")), List.of(this.amount, this.percentOfGT))
            .build();

    result = this.executor.executeQuery(query);
    gt = 7.1d;
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("car", "eu", "france", "lyon", 0.1d, 0.1d / gt),
            Arrays.asList("car", "eu", "france", "paris", 1d, 1d / gt),
            Arrays.asList("hobbies", "eu", "france", "lyon", 1d, 1d / gt),
            Arrays.asList("hobbies", "eu", "france", "paris", 1d, 1d / gt),
            Arrays.asList("home", "eu", "france", "lyon", 2d, 2d / gt),
            Arrays.asList("home", "eu", "france", "paris", 2d, 2d / gt));
  }

  @Test
  void testFiltersNotCleared() {
    ComparisonMeasureGrandTotal percentOfGT = new ComparisonMeasureGrandTotal("percentOfParent", ComparisonMethod.DIVIDE, this.amount);
    percentOfGT.clearFilters = false;
    List<Field> fields = tableFields(List.of("country", "city"));
    QueryDto query = Query
            .from(this.storeName)
            .where(Functions.criterion(tableField("country"), Functions.eq("france")))
            .select(fields, List.of(this.amount, percentOfGT))
            .rollup(fields)
            .build();

    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList(GRAND_TOTAL, GRAND_TOTAL, 7.1d, 1d),
            Arrays.asList("france", TOTAL, 7.1d, 1d),
            Arrays.asList("france", "lyon", 3.1d, .4366197183098592d),
            Arrays.asList("france", "paris", 4d, .5633802816901409d));
  }
}
