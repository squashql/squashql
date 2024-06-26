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
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestParentComparisonWithOtherColumn extends ABaseTestQuery {

  private final String storeName = "store" + getClass().getSimpleName().toLowerCase();

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
    Measure amount = Functions.sum("amount", "amount");
    final List<Field> fields = tableFields(List.of("continent", "country", "city"));
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", DIVIDE, amount, fields);
    QueryDto query = Query
            .from(this.storeName)
            .select(fields, List.of(amount, pOp))
            .build();

    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("eu", "france", "lyon", 3.1d, 3.1d / (4d + 3.1d)),
            Arrays.asList("eu", "france", "paris", 4d, 4d / (4d + 3.1d)),
            Arrays.asList("eu", "uk", "london", 9d, 1d));
  }

  @Test
  void testSkipMiddleAncestors() {
    Measure amount = Functions.sum("amount", "amount");
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", DIVIDE, amount, tableFields(List.of("continent", "country", "city")));
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("continent", "country", "city")), List.of(amount, pOp))
            .build();

    Table result = this.executor.executeQuery(query);
    // Note: contrary to what you might expect, the result here is the same as having List.of("city", "country","continent")
    // and it is not meant to compute the percent of the grandparent.
    Assertions.assertThat(result).containsExactly(
            Arrays.asList("eu", "france", "lyon", 3.1d, 3.1d / (4d + 3.1d)),
            Arrays.asList("eu", "france", "paris", 4d, 4d / (4d + 3.1d)),
            Arrays.asList("eu", "uk", "london", 9d, 1d));
  }

  @Test
  void testCrossjoinWithOtherColumn() {
    Measure amount = Functions.sum("amount", "amount");
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", DIVIDE, amount, tableFields(List.of("continent", "country", "city")));
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("spending_category", "continent", "country", "city")), List.of(amount, pOp))
            .build();

    Table result = this.executor.executeQuery(query);
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
    Measure amount = Functions.sum("amount", "amount");
    ComparisonMeasureReferencePosition pOp = new ComparisonMeasureReferencePosition("percentOfParent", DIVIDE, amount, tableFields(List.of("city", "country", "continent")));
    QueryDto query = Query
            .from(this.storeName)
            .select(tableFields(List.of("spending_category", "city")), List.of(amount, pOp))
            .build();

    Table result = this.executor.executeQuery(query);
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

  /**
   * See {@link ATestParentComparison#testSimplePivotTable()}.
   */
  @Test
  void testSimplePivotTable() {
    Measure amount = Functions.sum("amount", "amount");
    PartialHierarchicalComparisonMeasure pOp = new PartialHierarchicalComparisonMeasure("percentOfParent2", DIVIDE, false, amount, Axis.COLUMN, false);
    QueryDto query = Query
            .from(this.storeName)
            .where(Functions.criterion(tableField("city"), Functions.in("paris"))) // to reduce the size of the table
            .select(tableFields(List.of("spending_category", "city")), List.of(amount, pOp))
            .build();

    PivotTableQueryDto ptQuery = new PivotTableQueryDto(query, tableFields(List.of("spending_category")), tableFields(List.of("city")), tableFields(List.of("spending_category")));
    PivotTable pivotTable = this.executor.executePivotQuery(ptQuery);
    Assertions.assertThat(pivotTable.table).containsExactly(
            Arrays.asList("car", GRAND_TOTAL, 1d, 0.25d),
            Arrays.asList("car", "paris", 1d, 0.25d),
            Arrays.asList("hobbies", GRAND_TOTAL, 1d, 0.25d),
            Arrays.asList("hobbies", "paris", 1d, 0.25d),
            Arrays.asList("home", GRAND_TOTAL, 2d, 0.5d),
            Arrays.asList("home", "paris", 2d, 0.5d));
  }
}
