package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.*;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestExperimentalQueryResultMerge extends ABaseTestQuery {

  String storeA = "StoreA";// + getClass().getSimpleName().toLowerCase();
  String storeB = "StoreB";// + getClass().getSimpleName().toLowerCase();
  Field category = new TableField(this.storeA, "category");
  Field idA = new TableField(this.storeA, "idA");
  Field idStoreA = new TableField(this.storeA, "id");
  Field priceA = new TableField(this.storeA, "priceA");
  Field idB = new TableField(this.storeB, "idB");
  Field idStoreB = new TableField(this.storeB, "id");
  Field priceB = new TableField(this.storeB, "priceB");
  Measure priceASum = Functions.sum("priceA", this.priceA);
  Measure priceBSum = Functions.sum("priceB", this.priceB);

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField category = new TableTypedField(this.storeA, "category", String.class);
    TableTypedField idA = new TableTypedField(this.storeA, "idA", String.class);
    TableTypedField idStoreA = new TableTypedField(this.storeA, "idStoreA", String.class);
    TableTypedField priceA = new TableTypedField(this.storeA, "priceA", double.class);

    TableTypedField idB = new TableTypedField(this.storeB, "idB", String.class);
    TableTypedField idStoreB = new TableTypedField(this.storeB, "idStoreB", String.class);
    TableTypedField priceB = new TableTypedField(this.storeB, "priceB", double.class);
    return Map.of(this.storeA, List.of(category, idA, idStoreA, priceA), this.storeB, List.of(idB, idStoreB, priceB));
  }

  @Override
  protected void loadData() {
    this.tm.load(this.storeA, List.of(
            new Object[]{"A", "0", "0", 1d},
            new Object[]{"A", "1", "1", 2d},
            new Object[]{"B", "0", "0", 3d},
            new Object[]{"B", "3", "3", 4d}
    ));
    this.tm.load(this.storeB, List.of(
            new Object[]{"0", "0", 10d},
            new Object[]{"1", "1", 20d},
            new Object[]{"2", "2", 30d}
    ));
  }

  @Test
  void testLeftJoinWithDifferentColumns() {
    QueryDto queryL = Query
            .from(this.storeA)
            .select(List.of(this.category, this.idA), List.of(this.priceASum))
            .build();

    QueryDto queryR = Query
            .from(this.storeB)
            .select(List.of(this.idB), List.of(this.priceBSum))
            .build();

    SimpleOrderDto asc = new SimpleOrderDto(OrderKeywordDto.ASC);
    Map<Field, OrderDto> orders = new LinkedHashMap<>(); // order matters
    orders.put(this.category, asc);
    orders.put(this.idA, asc);
    Table result = this.executor.executeExperimentalQueryMerge(
            queryL, queryR, JoinType.LEFT,
            Functions.criterion(this.idB, this.idA, ConditionType.EQ),
            orders,
            -1,
            null);
    Assertions.assertThat(result.headers().stream().map(header -> header.name()).toList())
            .containsExactly("category", "idA", "priceA", "idB", "priceB");
    Assertions.assertThat(result).containsExactly(
            List.of("A", "0", 1d, "0", 10d),
            List.of("A", "1", 2d, "1", 20d),
            List.of("B", "0", 3d, "0", 10d),
            Arrays.asList("B", "3", 4d, null, getDoubleNullJoinValue()));
  }

  @Test
  void testLeftJoinWithCommonColumns() {
    QueryDto queryL = Query
            .from(this.storeA)
            .select(List.of(this.idA), List.of(this.priceASum))
            .build();

    QueryDto queryR = Query
            .from(this.storeB)
            .select(List.of(this.idB), List.of(this.priceBSum))
            .build();

    SimpleOrderDto asc = new SimpleOrderDto(OrderKeywordDto.ASC);
    Table result = this.executor.executeExperimentalQueryMerge(
            queryL, queryR, JoinType.LEFT,
            Functions.criterion(this.idB, this.idA, ConditionType.EQ),
            Map.of(this.idA, asc),
            -1,
            null);
    Assertions.assertThat(result.headers().stream().map(header -> header.name()).toList())
            .containsExactly("idA", "priceA", "idB", "priceB");
    Assertions.assertThat(result).containsExactly(
            List.of("0", 4d, "0", 10d),
            List.of("1", 2d, "1", 20d),
            Arrays.asList("3", 4d, null, getDoubleNullJoinValue()));
  }

  /**
   * Returns the value of a double when the join fails.
   */
  private Object getDoubleNullJoinValue() {
    String qesn = this.executor.queryEngine.getClass().getSimpleName();
    if (qesn.toLowerCase().contains(TestClass.Type.SNOWFLAKE.name().toLowerCase())) {
      return 0d;
    } else if (qesn.toLowerCase().contains(TestClass.Type.DUCKDB.name().toLowerCase())) {
      return Double.NaN;
    } else {

      return null;
    }
  }

  // TODO test with colonnes with same names => if same name, SquashQL can remove those columns to keep only 1
}
