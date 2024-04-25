package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.ConditionType;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static io.squashql.query.Functions.*;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestSQLFunction extends ABaseTestQuery {

  private final String storeName = "store" + ATestSQLFunction.class.getSimpleName().toLowerCase();
  private final TableField ean = new TableField(this.storeName, "ean");
  private final TableField category = new TableField(this.storeName, "category");
  private final TableField price = new TableField(this.storeName, "price");
  private final TableField date = new TableField(this.storeName, "date");
  private final LocalDate now = LocalDate.now();

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField ean = new TableTypedField(this.storeName, "ean", String.class);
    TableTypedField category = new TableTypedField(this.storeName, "category", String.class);
    TableTypedField price = new TableTypedField(this.storeName, "price", double.class);
    TableTypedField date = new TableTypedField(this.storeName, "date", LocalDate.class);

    return Map.of(this.storeName, List.of(ean, category, price, date));
  }

  @Override
  protected void loadData() {
    // In a test, we are testing current date. So we need to make sure the result is predictable
    this.tm.load(this.storeName, List.of(
            new Object[]{"0", "drink", 2d, this.now.minusMonths(1)},
            new Object[]{"1", "DrinK", 1d, this.now.plusMonths(1)},
            new Object[]{"2", "food", 3d, this.now.plusMonths(2)},
            new Object[]{"3", "cloth", 10d, this.now.plusMonths(3)}
    ));
  }

  /**
   * Test {@link Functions#lower(Field)}
   */
  @Test
  void testLower() {
    // lower in where
    QueryDto query = Query
            .from(this.storeName)
            .where(criterion(lower(this.category), eq("drink")))
            .select(List.of(this.category), List.of(CountMeasure.INSTANCE))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name)).containsExactly(SqlUtils.squashqlExpression(this.category), CountMeasure.INSTANCE.alias);
    Assertions.assertThat(result).containsExactly(
            List.of("DrinK", 1L),
            List.of("drink", 1L));

    // lower in select and where
    query = Query
            .from(this.storeName)
            .where(criterion(lower(this.category), eq("drink")))
            .select(List.of(lower(this.category)), List.of(CountMeasure.INSTANCE))
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name)).containsExactly("lower(" + SqlUtils.squashqlExpression(this.category) + ")", CountMeasure.INSTANCE.alias);
    Assertions.assertThat(result).containsExactly(List.of("drink", 2L));

    // lower in select and where + alias
    Field lower = lower(this.category).as("lower");
    query = Query
            .from(this.storeName)
            .where(criterion(lower, eq("drink")))
            .select(List.of(lower), List.of(CountMeasure.INSTANCE))
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name)).containsExactly("lower", CountMeasure.INSTANCE.alias);
    Assertions.assertThat(result).containsExactly(List.of("drink", 2L));
  }

  /**
   * Test {@link Functions#upper(Field)}
   */
  @Test
  void testUpper() {
    // upper in where
    QueryDto query = Query
            .from(this.storeName)
            .where(criterion(upper(this.category), eq("DRINK")))
            .select(List.of(this.category), List.of(CountMeasure.INSTANCE))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name)).containsExactly(SqlUtils.squashqlExpression(this.category), CountMeasure.INSTANCE.alias);
    Assertions.assertThat(result).containsExactly(
            List.of("DrinK", 1L),
            List.of("drink", 1L));

    // upper in select and where
    query = Query
            .from(this.storeName)
            .where(criterion(upper(this.category), eq("DRINK")))
            .select(List.of(upper(this.category)), List.of(CountMeasure.INSTANCE))
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name)).containsExactly("upper(" + SqlUtils.squashqlExpression(this.category) + ")", CountMeasure.INSTANCE.alias);
    Assertions.assertThat(result).containsExactly(List.of("DRINK", 2L));

    // upper in select and where + alias
    Field upper = upper(this.category).as("upper");
    query = Query
            .from(this.storeName)
            .where(criterion(upper, eq("DRINK")))
            .select(List.of(upper), List.of(CountMeasure.INSTANCE))
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name)).containsExactly("upper", CountMeasure.INSTANCE.alias);
    Assertions.assertThat(result).containsExactly(List.of("DRINK", 2L));
  }

  @Test
  void testCurrentDate() {
    // currentDate in where
    QueryDto query = Query
            .from(this.storeName)
            .where(criterion(this.date, Functions.currentDate(), ConditionType.LT))
            .select(List.of(this.ean), List.of(CountMeasure.INSTANCE))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name)).containsExactly(SqlUtils.squashqlExpression(this.ean), CountMeasure.INSTANCE.alias);
    Assertions.assertThat(result).containsExactly(List.of("0", 1L));

    // currentDate in select and where. This query does not make any sense...
    query = Query
            .from(this.storeName)
            .where(criterion(this.date, Functions.currentDate(), ConditionType.LT))
            .select(List.of(Functions.currentDate()), List.of(CountMeasure.INSTANCE))
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name)).containsExactly("current_date()", CountMeasure.INSTANCE.alias);
    // We cannot assert the date sent back because the test and the db are not guarantee to run in the same timezone
    //    Assertions.assertThat(result).containsExactly(List.of(this.now, 1L));
    Assertions.assertThat(result.count()).isEqualTo(1L);

    // currentDate in select and where + alias. This query does not make any sense...
    Field cr = currentDate().as("CR");
    query = Query
            .from(this.storeName)
            .where(criterion(this.date, cr, ConditionType.LT))
            .select(List.of(cr), List.of(CountMeasure.INSTANCE))
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result.headers().stream().map(Header::name)).containsExactly("CR", CountMeasure.INSTANCE.alias);
    // We cannot assert the date sent back because the test and the db are not guarantee to run in the same timezone
    //    Assertions.assertThat(result).containsExactly(List.of(this.now, 1L));
    Assertions.assertThat(result.count()).isEqualTo(1L);
  }
}
