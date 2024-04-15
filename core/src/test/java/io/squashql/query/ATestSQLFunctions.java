package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.database.SqlUtils;
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
import static io.squashql.query.Functions.upper;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestSQLFunctions extends ABaseTestQuery {

  private final String storeName = "store" + ATestSQLFunctions.class.getSimpleName().toLowerCase();
  private final TableField ean = new TableField(this.storeName, "ean");
  private final TableField category = new TableField(this.storeName, "category");
  private final TableField price = new TableField(this.storeName, "price");
  private final TableField date = new TableField(this.storeName, "date");

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
    this.tm.load(this.storeName, List.of(
            new Object[]{"0", "drink", 2d, LocalDate.of(2024, 1, 1)},
            new Object[]{"1", "DrinK", 1d, LocalDate.of(2024, 2, 1)},
            new Object[]{"2", "food", 3d, LocalDate.of(2024, 1, 10)},
            new Object[]{"3", "cloth", 10d, LocalDate.of(2024, 4, 1)}
    ));
  }

  /**
   * Test {@link Functions#lower(Field)}
   */
  @Test
  void testLCase() {
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
    Assertions.assertThat(result.headers().stream().map(Header::name)).containsExactly("LCase(" + SqlUtils.squashqlExpression(this.category) + ")", CountMeasure.INSTANCE.alias);
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
  void testUCase() {
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
    Assertions.assertThat(result.headers().stream().map(Header::name)).containsExactly("UCase(" + SqlUtils.squashqlExpression(this.category) + ")", CountMeasure.INSTANCE.alias);
    Assertions.assertThat(result).containsExactly(List.of("DRINK", 2L));

    // lower in select and where + alias
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
}
