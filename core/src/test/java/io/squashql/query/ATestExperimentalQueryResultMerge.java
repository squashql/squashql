package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.*;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.*;

import static io.squashql.query.Functions.*;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestExperimentalQueryResultMerge extends ABaseTestQuery {

  String storeA = "StoreA"; // + getClass().getSimpleName().toLowerCase();
  String storeB = "StoreB"; // + getClass().getSimpleName().toLowerCase();
  String storeC = "StoreC"; // + getClass().getSimpleName().toLowerCase();
  Field category = new TableField(this.storeA, "category");
  Field idA = new TableField(this.storeA, "idA");
  Field idStoreA = new TableField(this.storeA, "id");
  Field priceA = new TableField(this.storeA, "priceA");
  Field idB = new TableField(this.storeB, "idB");
  Field idStoreB = new TableField(this.storeB, "id");
  Field priceB = new TableField(this.storeB, "priceB");
  Field idC = new TableField(this.storeC, "idC");
  Field idStoreC = new TableField(this.storeC, "id");
  Field priceC = new TableField(this.storeC, "priceC");
  Measure priceASum = Functions.sum("priceA", this.priceA);
  Measure priceBSum = Functions.sum("priceB", this.priceB);
  Measure priceCSum = Functions.sum("priceC", this.priceC);

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField category = new TableTypedField(this.storeA, "category", String.class);
    TableTypedField idA = new TableTypedField(this.storeA, "idA", String.class);
    TableTypedField idStoreA = new TableTypedField(this.storeA, "id", String.class);
    TableTypedField priceA = new TableTypedField(this.storeA, "priceA", double.class);

    TableTypedField idB = new TableTypedField(this.storeB, "idB", String.class);
    TableTypedField idStoreB = new TableTypedField(this.storeB, "id", String.class);
    TableTypedField priceB = new TableTypedField(this.storeB, "priceB", double.class);

    TableTypedField idC = new TableTypedField(this.storeC, "idC", String.class);
    TableTypedField idStoreC = new TableTypedField(this.storeC, "id", String.class);
    TableTypedField priceC = new TableTypedField(this.storeC, "priceC", double.class);
    return Map.of(
            this.storeA, List.of(category, idA, idStoreA, priceA),
            this.storeB, List.of(idB, idStoreB, priceB),
            this.storeC, List.of(idC, idStoreC, priceC));
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
    this.tm.load(this.storeC, List.of(
            new Object[]{"0", "0", 123d},
            new Object[]{"1", "1", 42d},
            new Object[]{"2", "2", 321d}
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
            criterion(this.idB, this.idA, ConditionType.EQ),
            orders,
            -1);
    Assertions.assertThat(result.headers().stream().map(Header::name).toList())
            .containsExactly(this.storeA + ".category", this.storeA + ".idA", "priceA", "priceB");
    Assertions.assertThat(result).containsExactly(
            List.of("A", "0", 1d, 10d),
            List.of("A", "1", 2d, 20d),
            List.of("B", "0", 3d, 10d),
            Arrays.asList("B", "3", 4d, getDoubleNullJoinValue()));

    result = this.executor.executeExperimentalQueryMerge(
            queryL, queryR, JoinType.LEFT,
            criterion(this.idB, this.idA, ConditionType.EQ),
            orders,
            1); // with limit
    Assertions.assertThat(result).containsExactly(List.of("A", "0", 1d, 10d));
  }

  @Test
  void testLeftJoinWithCommonColumnsDifferentNames() {
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
            criterion(this.idB, this.idA, ConditionType.EQ),
            Map.of(this.idA, asc),
            -1);
    Assertions.assertThat(result.headers().stream().map(Header::name).toList())
            .containsExactly(this.storeA + ".idA", "priceA", "priceB");
    Assertions.assertThat(result).containsExactly(
            List.of("0", 4d, 10d),
            List.of("1", 2d, 20d),
            Arrays.asList("3", 4d, getDoubleNullJoinValue()));
  }

  @Test
  void testLeftJoinWithCommonColumnsAndSameNames() {
    QueryDto queryL = Query
            .from(this.storeA)
            .select(List.of(this.idStoreA), List.of(this.priceASum))
            .build();

    QueryDto queryR = Query
            .from(this.storeB)
            .select(List.of(this.idStoreB), List.of(this.priceBSum))
            .build();

    SimpleOrderDto asc = new SimpleOrderDto(OrderKeywordDto.ASC);
    Table result = this.executor.executeExperimentalQueryMerge(
            queryL, queryR, JoinType.LEFT,
            criterion(this.idStoreB, this.idStoreA, ConditionType.EQ),
            Map.of(this.idStoreA, asc),
            -1);
    Assertions.assertThat(result.headers().stream().map(Header::name).toList())
            .containsExactly(this.storeA + ".id", "priceA", "priceB");
    Assertions.assertThat(result).containsExactly(
            List.of("0", 4d, 10d),
            List.of("1", 2d, 20d),
            Arrays.asList("3", 4d, getDoubleNullJoinValue()));
  }

  /**
   * Tests that when a field that is aliased, the corresponding is kept in the result. See {@link #testLeftJoinWithCommonColumnsAndSameNames()}
   * where the names are the same.
   */
  @Test
  void testLeftJoinWithCommonColumnsAndSameNamesWithAliases() {
    Field categoryAliased = this.category.as("category_aliased");
    QueryDto queryL = Query
            .from(this.storeA)
            .select(List.of(categoryAliased, this.idStoreA), List.of(this.priceASum))
            .build();

    Field idStoreBAliased = this.idStoreB.as("id_aliased");
    QueryDto queryR = Query
            .from(this.storeB)
            .select(List.of(idStoreBAliased), List.of(this.priceBSum))
            .build();

    SimpleOrderDto asc = new SimpleOrderDto(OrderKeywordDto.ASC);
    Map<Field, OrderDto> orders = new LinkedHashMap<>(); // order matters
    orders.put(categoryAliased, asc);
    orders.put(idStoreBAliased, asc);
    Table result = this.executor.executeExperimentalQueryMerge(
            queryL, queryR, JoinType.LEFT,
            criterion(idStoreBAliased, this.idStoreA, ConditionType.EQ), // use the aliased in the join condition
            orders,  // use the aliased in the order by condition
            -1);
    Assertions.assertThat(result.headers().stream().map(Header::name).toList())
            .containsExactly("category_aliased", this.storeA + ".id", "priceA", "priceB");
    Assertions.assertThat(result).containsExactly(
            List.of("A", "0", 1d, 10d),
            List.of("A", "1", 2d, 20d),
            List.of("B", "0", 3d, 10d),
            Arrays.asList("B", "3", 4d, getDoubleNullJoinValue()));
  }

  @Test
  void testLeftJoinWithMultipleConditions() {
    QueryDto queryL = Query
            .from(this.storeA)
            .select(List.of(this.idA, this.idStoreA), List.of(this.priceASum))
            .build();

    QueryDto queryR = Query
            .from(this.storeB)
            .select(List.of(this.idB, this.idStoreB), List.of(this.priceBSum))
            .build();

    SimpleOrderDto asc = new SimpleOrderDto(OrderKeywordDto.ASC);
    Table result = this.executor.executeExperimentalQueryMerge(
            queryL, queryR, JoinType.LEFT,
            all(criterion(this.idB, this.idA, ConditionType.EQ), criterion(this.idStoreB, this.idStoreA, ConditionType.EQ)),
            Map.of(this.idA, asc),
            -1);
    Assertions.assertThat(result.headers().stream().map(Header::name).toList())
            .containsExactly(this.storeA + ".idA", this.storeA + ".id", "priceA", "priceB");
    Assertions.assertThat(result).containsExactly(
            List.of("0", "0", 4d, 10d),
            List.of("1", "1", 2d, 20d),
            Arrays.asList("3", "3", 4d, getDoubleNullJoinValue()));
  }

  @Test
  void testLeftJoinWithoutCriteriaWithColumnsInCommon() {
    String firstKey = "first_key";
    String secondKey = "second_key";
    Field idA = this.idA.as(firstKey);
    Field idStoreA = this.idStoreA.as(secondKey);
    QueryDto queryL = Query
            .from(this.storeA)
            .select(List.of(idA, idStoreA), List.of(this.priceASum))
            .build();

    QueryDto queryR = Query
            .from(this.storeB)
            .select(List.of(this.idB.as(firstKey), this.idStoreB.as(secondKey)), List.of(this.priceBSum))
            .build();

    SimpleOrderDto asc = new SimpleOrderDto(OrderKeywordDto.ASC);
    Table result = this.executor.executeExperimentalQueryMerge(
            queryL, queryR, JoinType.LEFT,
            null,
            Map.of(idA, asc),
            -1);
    Assertions.assertThat(result.headers().stream().map(Header::name).toList())
            .containsExactly(firstKey, secondKey, "priceA", "priceB");
    Assertions.assertThat(result).containsExactly(
            List.of("0", "0", 4d, 10d),
            List.of("1", "1", 2d, 20d),
            Arrays.asList("3", "3", 4d, getDoubleNullJoinValue()));
  }

  @Test
  void testLeftJoinWithoutCriteriaAndNoColumnInCommon() {
    QueryDto queryL = Query
            .from(this.storeA)
            .select(List.of(), List.of(this.priceASum))
            .build();

    QueryDto queryR = Query
            .from(this.storeB)
            .select(List.of(), List.of(this.priceBSum))
            .build();

    SimpleOrderDto asc = new SimpleOrderDto(OrderKeywordDto.ASC);
    Table result = this.executor.executeExperimentalQueryMerge(
            queryL, queryR, JoinType.CROSS,
            null,
            null,
            -1);
    Assertions.assertThat(result.headers().stream().map(Header::name).toList())
            .containsExactly("priceA", "priceB");
    Assertions.assertThat(result).containsExactly(List.of(10d, 60d));
  }

  @Test
  void testWithSubQueries() {
    Field idAliasedA = this.idStoreA.as("id_aliased_a");
    QueryDto queryL = Query
            .from(this.storeA)
            .select(List.of(this.idA, idAliasedA), List.of(this.priceASum))
            .build();

    // This query does not make sense, but it is to make sure there is no issue when using sub-query.
    queryL = Query.from(queryL)
            .select(List.of(new AliasedField(idAliasedA.alias())), List.of(sum("priceA2", new AliasedField(this.priceASum.alias()))))
            .build();

    Field idAliasedB = this.idStoreB.as("id_aliased_b");
    QueryDto queryR = Query
            .from(this.storeB)
            .select(List.of(this.idB, idAliasedB), List.of(this.priceBSum))
            .build();

    queryR = Query
            .from(queryR)
            .select(List.of(new AliasedField(idAliasedB.alias())), List.of(sum("priceB2", new AliasedField(this.priceBSum.alias()))))
            .build();

    // In case of sub-queries, aliases must be used everywhere.
    SimpleOrderDto asc = new SimpleOrderDto(OrderKeywordDto.ASC);
    Table result = this.executor.executeExperimentalQueryMerge(
            queryL, queryR, JoinType.LEFT,
            criterion(idAliasedB, idAliasedA, ConditionType.EQ),
            Map.of(idAliasedA, asc),
            -1);
    Assertions.assertThat(result.headers().stream().map(Header::name).toList())
            .containsExactly(idAliasedA.alias(), "priceA2", "priceB2");
    Assertions.assertThat(result).containsExactly(
            List.of("0", 4d, 10d),
            List.of("1", 2d, 20d),
            Arrays.asList("3", 4d, getDoubleNullJoinValue()));
  }

  @Test
  void testJoinWithMultipleQueries() {
    QueryDto queryA = Query
            .from(this.storeA)
            .select(List.of(this.category, this.idA), List.of(this.priceASum))
            .build();

    QueryDto queryB = Query
            .from(this.storeB)
            .select(List.of(this.idB), List.of(this.priceBSum))
            .build();

    QueryDto queryC = Query
            .from(this.storeC)
            .select(List.of(this.idC), List.of(this.priceCSum))
            .build();

    JoinStatement join = JoinStatement.start(queryA)
            .join(queryB, JoinType.INNER, criterion(this.idB, this.idA, ConditionType.EQ))
            .join(queryC, JoinType.LEFT, criterion(this.idB, this.idC, ConditionType.EQ));

    Table result = this.executor.executeExperimentalQueryMerge(
            queryA, queryB, JoinType.LEFT,
            criterion(this.idB, this.idA, ConditionType.EQ),
            Map.of(),
            -1);
    result.show();
    Assertions.fail("");
  }

  @Test
  void testLeftJoinWithoutCriteriaWithColumnsInCommonUsingSubquery() {
    QueryDto queryL = Query
            .from(this.storeA)
            .select(List.of(this.idStoreA), List.of(this.priceASum))
            .build();

    // This query does not make sense, but it is to make sure there is no issue when using sub-query.
    queryL = Query.from(queryL)
            .select(List.of(new TableField("id")) /* We can also use AliasedField */, List.of(sum("priceA2", new AliasedField(this.priceASum.alias()))))
            .build();

    QueryDto queryR = Query
            .from(this.storeB)
            .select(List.of(this.idStoreB), List.of(this.priceBSum))
            .build();

    queryR = Query
            .from(queryR)
            .select(List.of(new TableField("id")), List.of(sum("priceB2", new AliasedField(this.priceBSum.alias()))))
            .build();

    // In case of sub-queries, aliases must be used everywhere.
    Table result = this.executor.executeExperimentalQueryMerge(
            queryL, queryR, JoinType.LEFT,
            null,
            Map.of(),
            -1);
    Assertions.assertThat(result.headers().stream().map(Header::name).toList())
            .containsExactly("id", "priceA2", "priceB2");
    Assertions.assertThat(result).containsExactly(
            List.of("0", 4d, 10d),
            List.of("1", 2d, 20d),
            Arrays.asList("3", 4d, getDoubleNullJoinValue()));
  }

  public static class JoinStatement {

    List<QueryDto> queries = new ArrayList<>();
    TableDto tableDto;

    int current = 0;

    private JoinStatement(QueryDto q1) {
      this.queries.add(q1);
      this.tableDto = new TableDto(String.format("__cte%d__", this.current++));
    }

    public static JoinStatement start(QueryDto q) {
      return new JoinStatement(q);
    }

    public JoinStatement join(QueryDto q, JoinType joinType, CriteriaDto criteriaDto) {
      this.queries.add(q);
      this.tableDto.join(new TableDto(String.format("__cte%d__", this.current++)), joinType, criteriaDto);
      return this;
    }
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
}
