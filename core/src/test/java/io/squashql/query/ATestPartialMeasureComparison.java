package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.field.Field;
import io.squashql.query.field.TableField;
import io.squashql.query.measure.Measure;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.squashql.query.Axis.COLUMN;
import static io.squashql.query.ComparisonMethod.DIVIDE;
import static io.squashql.query.Functions.compareWithParentOfAxisMeasure;
import static io.squashql.query.Functions.compareWithTotalOfAxisMeasure;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;
import static io.squashql.transaction.DataLoader.MAIN_SCENARIO_NAME;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestPartialMeasureComparison extends ABaseTestQuery {

  private final String storeName = "store" + getClass().getSimpleName().toLowerCase();
  private final TableField city = new TableField(this.storeName, "city");
  private final TableField country = new TableField(this.storeName, "country");
  private final TableField continent = new TableField(this.storeName, "continent");
  private final TableField spendingCategory = new TableField(this.storeName, "spending_category");
  private final TableField amount = new TableField(this.storeName, "amount");

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
    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
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
  void testPercentOfParent() {
    Measure pop = Functions.sum("amount", this.amount);
    List<Field> fields = List.of(this.continent, this.country, this.city);
    Measure pOp = compareWithParentOfAxisMeasure("percentOfParent", DIVIDE, pop, COLUMN);
    QueryDto query = Query
            .from(this.storeName)
            .select(fields, List.of(pop, pOp))
            .rollup(fields)
            .build();

    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList(GRAND_TOTAL, GRAND_TOTAL, GRAND_TOTAL, 16.1d, 1d),
            Arrays.asList("eu", TOTAL, TOTAL, 16.1d, 16.1d / 16.1d),
            Arrays.asList("eu", "france", TOTAL, 7.1d, 7.1d / 16.1d),
            Arrays.asList("eu", "france", "lyon", 3.1d, 3.1d / 7.1d),
            Arrays.asList("eu", "france", "paris", 4d, 4d / 7.1d),
            Arrays.asList("eu", "uk", TOTAL, 9d, 9d / 16.1d),
            Arrays.asList("eu", "uk", "london", 9d, 9d / 9d));
  }

  @Test
  void testPercentOfTotalOfAxis() {
    Measure pop = Functions.sum("amount", this.amount);
    List<Field> fields = List.of(this.continent, this.country, this.city);
    Measure pOp = compareWithTotalOfAxisMeasure("percentOfParent", DIVIDE, pop, COLUMN);
    QueryDto query = Query
            .from(this.storeName)
            .select(fields, List.of(pop, pOp))
            .rollup(fields)
            .build();

    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            Arrays.asList(GRAND_TOTAL, GRAND_TOTAL, GRAND_TOTAL, 16.1d, 1d),
            Arrays.asList("eu", TOTAL, TOTAL, 16.1d, 16.1d / 16.1d),
            Arrays.asList("eu", "france", TOTAL, 7.1d, 7.1d / 16.1d),
            Arrays.asList("eu", "france", "lyon", 3.1d, 3.1d / 16.1),
            Arrays.asList("eu", "france", "paris", 4d, 4d / 16.1),
            Arrays.asList("eu", "uk", TOTAL, 9d, 9d / 16.1d),
            Arrays.asList("eu", "uk", "london", 9d, 9d / 16.1));
  }

  // TODO test with PT.
}
