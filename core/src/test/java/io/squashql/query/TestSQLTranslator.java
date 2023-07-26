package io.squashql.query;

import io.squashql.query.database.AQueryEngine;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.DefaultQueryRewriter;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.*;
import io.squashql.store.TypedField;
import io.squashql.store.Store;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.squashql.query.Functions.*;
import static io.squashql.query.database.SQLTranslator.translate;
import static io.squashql.query.dto.JoinType.INNER;
import static io.squashql.query.dto.JoinType.LEFT;
import static io.squashql.transaction.DataLoader.SCENARIO_FIELD_NAME;

public class TestSQLTranslator {

  private static final String BASE_STORE_NAME = "baseStore";
  private static final String BASE_STORE_NAME_ESCAPED = SqlUtils.backtickEscape(BASE_STORE_NAME);

  /**
   * A simple implementation that has the same logic as {@link AQueryEngine#createFieldSupplier(Map)} but does not verify
   * a store/field does exist.
   */
  private static final Function<String, TypedField> fp = s -> {
    Function<String, Class<?>> type = f -> switch (f) {
      case "pnl", BASE_STORE_NAME + "." + "pnl" -> double.class;
      case "delta", BASE_STORE_NAME + "." + "delta" -> Double.class;
      default -> String.class;
    };
    String[] split = s.split("\\.");
    if (split.length > 1) {
      String tableName = split[0];
      String fieldNameInTable = split[1];
      return new TypedField(tableName, fieldNameInTable, type.apply(fieldNameInTable));
    } else {
      return new TypedField(null, s, type.apply(s));
    }
  };

  @Test
  void testGrandTotal() {
    DatabaseQuery query = new DatabaseQuery()
            .aggregatedMeasure("pnl.sum", "pnl", "sum")
            .aggregatedMeasure("delta.sum", "delta", "sum")
            .aggregatedMeasure("pnl.avg", "pnl", "avg")
            .aggregatedMeasure("mean pnl", "pnl", "avg")
            .table(BASE_STORE_NAME);

    Assertions.assertThat(translate(query, fp))
            .isEqualTo("select sum(`pnl`) as `pnl.sum`, sum(`delta`) as `delta.sum`, avg(`pnl`) as `pnl.avg`, avg(`pnl`) as `mean pnl` from " + BASE_STORE_NAME_ESCAPED);
  }

  @Test
  void testLimit() {
    DatabaseQuery query = new DatabaseQuery()
            .aggregatedMeasure("pnl.sum", "pnl", "sum")
            .limit(8)
            .table(BASE_STORE_NAME);

    Assertions.assertThat(translate(query, fp))
            .isEqualTo("select sum(`pnl`) as `pnl.sum` from `" + BASE_STORE_NAME + "` limit 8");
  }

  @Test
  void testGroupBy() {
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(fp.apply(SCENARIO_FIELD_NAME))
            .withSelect(fp.apply("type"))
            .aggregatedMeasure("pnl.sum", "pnl", "sum")
            .aggregatedMeasure("delta.sum", "delta", "sum")
            .aggregatedMeasure("pnl.avg", "pnl", "avg")
            .table(BASE_STORE_NAME);

    Assertions.assertThat(translate(query, fp))
            .isEqualTo("select `scenario`, `type`, sum(`pnl`) as `pnl.sum`, sum(`delta`) as `delta.sum`, avg(`pnl`) as `pnl.avg` from " + BASE_STORE_NAME_ESCAPED + " group by " +
                    "`scenario`, `type`");
  }

  @Test
  void testGroupByWithFullName() {
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(fp.apply(SqlUtils.getFieldFullName(BASE_STORE_NAME, SCENARIO_FIELD_NAME)))
            .withSelect(fp.apply(SqlUtils.getFieldFullName(BASE_STORE_NAME, "type")))
            .aggregatedMeasure("pnl.sum", SqlUtils.getFieldFullName(BASE_STORE_NAME, "pnl"), "sum")
            .aggregatedMeasure("delta.sum", SqlUtils.getFieldFullName(BASE_STORE_NAME, "delta"), "sum")
            .table(BASE_STORE_NAME);

    Assertions.assertThat(translate(query, fp))
            .isEqualTo("select `baseStore`.`scenario`, `baseStore`.`type`, sum(`baseStore`.`pnl`) as `pnl.sum`, sum(`baseStore`.`delta`) as `delta.sum` from " + BASE_STORE_NAME_ESCAPED + " group by " +
                    "`baseStore`.`scenario`, `baseStore`.`type`");
  }

  @Test
  void testDifferentMeasures() {
    DatabaseQuery query = new DatabaseQuery()
            .table(BASE_STORE_NAME)
            .aggregatedMeasure("pnl.sum", "pnl", "sum")
            .expressionMeasure("indice", "100 * sum(`delta`) / sum(`pnl`)");

    Assertions.assertThat(translate(query, fp))
            .isEqualTo("select sum(`pnl`) as `pnl.sum`, 100 * sum(`delta`) / sum(`pnl`) as `indice` from " + BASE_STORE_NAME_ESCAPED);
  }

  @Test
  void testWithFullRollup() {
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(fp.apply(SCENARIO_FIELD_NAME))
            .withSelect(fp.apply("type"))
            .rollup(List.of(fp.apply(SCENARIO_FIELD_NAME), fp.apply("type")))
            .aggregatedMeasure("pnl.sum", "price", "sum")
            .table(BASE_STORE_NAME);

    Assertions.assertThat(translate(query, fp))
            .isEqualTo("""
                    select `scenario`, `type`,
                     grouping(`scenario`), grouping(`type`),
                     sum(`price`) as `pnl.sum`
                     from `baseStore` group by rollup(`scenario`, `type`)
                    """.replaceAll(System.lineSeparator(), ""));
  }

  @Test
  void testWithFullRollupWithFullName() {
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(fp.apply(SqlUtils.getFieldFullName(BASE_STORE_NAME, SCENARIO_FIELD_NAME)))
            .withSelect(fp.apply(SqlUtils.getFieldFullName(BASE_STORE_NAME, "type")))
            .rollup(List.of(
                    fp.apply(SqlUtils.getFieldFullName(BASE_STORE_NAME, SCENARIO_FIELD_NAME)),
                    fp.apply(SqlUtils.getFieldFullName(BASE_STORE_NAME, "type"))))
            .aggregatedMeasure("pnl.sum", "price", "sum")
            .table(BASE_STORE_NAME);

    Assertions.assertThat(translate(query, fp))
            .isEqualTo("""
                    select `baseStore`.`scenario`, `baseStore`.`type`,
                     grouping(`baseStore`.`scenario`), grouping(`baseStore`.`type`),
                     sum(`price`) as `pnl.sum`
                     from `baseStore` group by rollup(`baseStore`.`scenario`, `baseStore`.`type`)
                    """.replaceAll(System.lineSeparator(), ""));
  }

  @Test
  void testWithPartialRollup() {
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(fp.apply(SCENARIO_FIELD_NAME))
            .withSelect(fp.apply("type"))
            .rollup(List.of(fp.apply(SCENARIO_FIELD_NAME)))
            .aggregatedMeasure("pnl.sum", "price", "sum")
            .table(BASE_STORE_NAME);

    Assertions.assertThat(translate(query, fp))
            .isEqualTo("select `scenario`, `type`," +
                    " grouping(`scenario`)," +
                    " sum(`price`) as `pnl.sum`" +
                    " from `baseStore` group by `type`, rollup(`scenario`)");
  }

  @Test
  void testJoins() {
    TableDto baseStore = new TableDto(BASE_STORE_NAME);
    TableDto table1 = new TableDto("table1");
    JoinMappingDto mappingBaseToTable1 = new JoinMappingDto(baseStore.name + ".id", table1.name + ".table1_id");
    TableDto table2 = new TableDto("table2");
    JoinMappingDto mappingBaseToTable2 = new JoinMappingDto(baseStore.name + ".id", table2.name + ".table2_id");
    TableDto table3 = new TableDto("table3");
    JoinMappingDto mappingTable2ToTable3 = new JoinMappingDto(table2.name + ".table2_field_1", table3.name + ".table3_id");
    TableDto table4 = new TableDto("table4");
    List<JoinMappingDto> mappingTable1ToTable4 = List.of(
            new JoinMappingDto(table1.name + ".table1_field_2", table4.name + ".table4_id_1"),
            new JoinMappingDto(table1.name + ".table1_field_3", table4.name + ".table4_id_2"));

    baseStore.joins.add(new JoinDto(table1, INNER, mappingBaseToTable1));
    baseStore.joins.add(new JoinDto(table2, LEFT, mappingBaseToTable2));

    table1.joins.add(new JoinDto(table4, INNER, mappingTable1ToTable4));
    table2.joins.add(new JoinDto(table3, INNER, mappingTable2ToTable3));

    DatabaseQuery query = new DatabaseQuery()
            .table(baseStore)
            .aggregatedMeasure("pnl.avg", "pnl", "avg");

    Assertions.assertThat(translate(query, fp))
            .isEqualTo("select avg(`pnl`) as `pnl.avg` from " + BASE_STORE_NAME_ESCAPED
                    + " inner join `table1` on " + BASE_STORE_NAME_ESCAPED + ".`id` = `table1`.`table1_id`"
                    + " inner join `table4` on `table1`.`table1_field_2` = `table4`.`table4_id_1` and `table1`.`table1_field_3` = `table4`.`table4_id_2`"
                    + " left join `table2` on " + BASE_STORE_NAME_ESCAPED + ".`id` = `table2`.`table2_id`"
                    + " inner join `table3` on `table2`.`table2_field_1` = `table3`.`table3_id`"
            );
  }

  @Test
  void testJoinsEquijoinsMultipleCondCrossTables() {
    TableDto a = new TableDto("A");
    TableDto b = new TableDto("B");
    JoinMappingDto jAToB = new JoinMappingDto(a.name + ".a_id", b.name + ".b_id");
    TableDto c = new TableDto("C");
    // should be able to ref. both column of A and B in the join.
    List<JoinMappingDto> jCToAB = List.of(
            new JoinMappingDto(c.name + ".c_other_id", b.name + ".b_other_id"),
            new JoinMappingDto(c.name + ".c_f", a.name + ".a_f"));

    a.join(b, INNER, jAToB);
    a.join(c, LEFT, jCToAB);

    Function<String, TypedField> fieldSupplier = AQueryEngine.createFieldSupplier(Map.of(
            "A", new Store("A", List.of(new TypedField("A", "a_id", int.class), new TypedField("A", "a_f", int.class), new TypedField("A", "y", int.class))),
            "B", new Store("B", List.of(new TypedField("B", "b_id", int.class), new TypedField("B", "b_other_id", int.class))),
            "C", new Store("C", List.of(new TypedField("c", "a_id", int.class), new TypedField("C", "c_f", int.class), new TypedField("C", "c_other_id", int.class)))
    ));

    DatabaseQuery query = new DatabaseQuery().table(a).withSelect(fieldSupplier.apply("A.y"));

    Assertions.assertThat(translate(query, fieldSupplier))
            .isEqualTo("select `A`.`y` from `A` " +
                    "inner join `B` on `A`.`a_id` = `B`.`b_id` " +
                    "left join `C` on `C`.`c_other_id` = `B`.`b_other_id` and `C`.`c_f` = `A`.`a_f` " +
                    "group by `A`.`y`");
  }

  @Test
  void testConditionsWithValue() {
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(fp.apply(SCENARIO_FIELD_NAME))
            .withSelect(fp.apply("type"))
            .aggregatedMeasure("pnl.sum", "pnl", "sum")
            .whereCriteria(all(
                    criterion(SCENARIO_FIELD_NAME, or(eq("base"), eq("s1"), eq("s2"))),
                    criterion("delta", ge(123d)),
                    criterion("type", or(eq("A"), eq("B"))),
                    criterion("pnl", lt(10d)))
            )
            .table(BASE_STORE_NAME);
    Assertions.assertThat(translate(query, fp))
            .isEqualTo("select `scenario`, `type`, sum(`pnl`) as `pnl.sum` from " + BASE_STORE_NAME_ESCAPED
                    + " where (((`scenario` = 'base' or `scenario` = 's1') or `scenario` = 's2')"
                    + " and `delta` >= 123.0 and (`type` = 'A' or `type` = 'B') and `pnl` < 10.0)"
                    + " group by `scenario`, `type`"
            );
  }

  @Test
  void testConditionWithValueFullPath() {
    TypedField field = new TypedField(BASE_STORE_NAME, SCENARIO_FIELD_NAME, String.class);
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(field)
            .aggregatedMeasure("pnl.sum", "pnl", "sum")
            .whereCriteria(criterion(SqlUtils.getFieldFullName(field), or(
                    eq("base"),
                    eq("s2"),
                    isNull())))
            .table(BASE_STORE_NAME);
    Assertions.assertThat(translate(query, fp))
            .isEqualTo("select `baseStore`.`scenario`, sum(`pnl`) as `pnl.sum` from " + BASE_STORE_NAME_ESCAPED
                    + " where ((`baseStore`.`scenario` = 'base' or `baseStore`.`scenario` = 's2') or `baseStore`.`scenario` is null)"
                    + " group by `baseStore`.`scenario`"
            );
  }

  @Test
  void testSelectFromSelect() {
    // Kind of leaf agg. !!!
    TableDto a = new TableDto("a");
    DatabaseQuery subQuery = new DatabaseQuery()
            .table(a)
            .withSelect(fp.apply("c1"))
            .withSelect(fp.apply("c3"))
            .withMeasure(avg("mean", "c2"));

    DatabaseQuery query = new DatabaseQuery()
            .subQuery(subQuery)
            .withSelect(fp.apply("c3")) // c3 needs to be in the subquery
            .withMeasure(sum("sum GT", "mean"))
            .whereCriteria(criterion("type", eq("myType")));
    Assertions.assertThat(translate(query, fp))
            .isEqualTo("select `c3`, sum(`mean`) as `sum GT` from (select `c1`, `c3`, avg(`c2`) as `mean` from `a` group by `c1`, `c3`) where `type` = 'myType' group by `c3`");
  }

  @Test
  void testBinaryOperationMeasure() {
    TableDto a = new TableDto("a");
    Measure plus = plus("plus", sum("pnl.sum", "pnl"), avg("delta.avg", "delta"));
    Measure divide = divide("divide", plus, Functions.decimal(100));
    DatabaseQuery query = new DatabaseQuery()
            .table(a)
            .withMeasure(plus)
            .withMeasure(divide);
    Assertions.assertThat(translate(query, fp))
            .isEqualTo("select (sum(`pnl`)+avg(`delta`)) as `plus`, ((sum(`pnl`)+avg(`delta`))/100.0) as `divide` from `a`");
  }

  @Test
  void testAggregatedMeasures() {
    TableDto a = new TableDto(BASE_STORE_NAME);
    DatabaseQuery query = new DatabaseQuery()
            .table(a)
            .withMeasure(sum("pnlSum", "pnl"))
            .withMeasure(sumIf("pnlSumFiltered", "pnl", criterion("country", eq("france"))));
    Assertions.assertThat(translate(query, fp))
            .isEqualTo("select sum(`pnl`) as `pnlSum`, sum(case when `country` = 'france' then `pnl` end) as `pnlSumFiltered` from `" + BASE_STORE_NAME + "`");

    // With full path
    query = new DatabaseQuery()
            .table(a)
            .withMeasure(sum("pnlSum", a.name + ".pnl"))
            .withMeasure(sumIf("pnlSumFiltered", a.name + ".pnl", criterion(a.name + ".country", eq("france"))));
    String format = "select sum(`%1$s`.`pnl`) as `pnlSum`, sum(case when `%1$s`.`country` = 'france' then `%1$s`.`pnl` end) as `pnlSumFiltered` from `%1$s`";
    Assertions.assertThat(translate(query, fp))
            .isEqualTo(String.format(
                    format, BASE_STORE_NAME));
  }

  @Test
  void testVirtualTable() {
    TableDto main = new TableDto(BASE_STORE_NAME);
    VirtualTableDto virtualTable = new VirtualTableDto(
            "virtual",
            List.of("a", "b"),
            List.of(List.of(0, "0"), List.of(1, "1")));
    main.join(new TableDto(virtualTable.name), INNER, new JoinMappingDto("id", "a", ConditionType.EQ));
    DatabaseQuery query = new DatabaseQuery()
            .table(main)
            .virtualTable(virtualTable)
            .withMeasure(sum("pnl.sum", "pnl"))
            .withSelect(fp.apply("id"))
            .withSelect(fp.apply("b"));
    String expected = String.format("with %2$s as (select 0 as `a`, '0' as `b` union all select 1 as `a`, '1' as `b`) " +
                    "select `id`, `b`, sum(`pnl`) as `pnl.sum` from `%1$s` " +
                    "inner join %2$s on `id` = `a` group by `id`, `b`",
            BASE_STORE_NAME, virtualTable.name);
    Assertions.assertThat(translate(query, fp))
            .isEqualTo(expected);
  }

  @Test
  void testVirtualTableFullName() {
    TableDto main = new TableDto(BASE_STORE_NAME);
    VirtualTableDto virtualTable = new VirtualTableDto(
            "virtual",
            List.of("a", "b"),
            List.of(List.of(0, "0"), List.of(1, "1")));
    main.join(new TableDto(virtualTable.name), INNER, new JoinMappingDto(BASE_STORE_NAME + ".id", virtualTable.name + ".a", ConditionType.EQ));
    DatabaseQuery query = new DatabaseQuery()
            .table(main)
            .virtualTable(virtualTable)
            .withMeasure(sum("pnl.sum", "pnl"))
            .withSelect(fp.apply("id"))
            .withSelect(fp.apply("b"));
    String expected = String.format("with %2$s as (select 0 as `a`, '0' as `b` union all select 1 as `a`, '1' as `b`) " +
                    "select `id`, `b`, sum(`pnl`) as `pnl.sum` from `%1$s` " +
                    "inner join %2$s on `%1$s`.`id` = %2$s.`a` group by `id`, `b`",
            BASE_STORE_NAME, virtualTable.name);
    // IMPORTANT: No backtick in the join condition for the field belonging to the virtual table.
    Assertions.assertThat(translate(query, fp))
            .isEqualTo(expected);
  }

  @Test
  void testGroupingSets() {
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(fp.apply("a"))
            .withSelect(fp.apply("b"))
            .aggregatedMeasure("pnl.sum", "pnl", "sum")
            .groupingSets(List.of(
                    List.of(), // GT
                    List.of(fp.apply("a")), // total a
                    List.of(fp.apply("a"), fp.apply("b"))
            ))
            .table(BASE_STORE_NAME);

    Assertions.assertThat(translate(query, fp))
            .isEqualTo("select `a`, `b`, grouping(`a`), grouping(`b`), sum(`pnl`) as `pnl.sum` from `baseStore` group by grouping sets((), (`a`), (`a`,`b`))");
  }

  @Test
  void testComplexAggregatedMeasures() {
    Field finalPrice = new TableField("recommendation.finalprice");
    Field recoPrice = new TableField("recommendation.recoprice");
    Measure measure = sumIf("increase_sum", minus(finalPrice, recoPrice), all(
            criterion(finalPrice, recoPrice, ConditionType.GT),
            criterion(recoPrice, gt(0))
    ));

    String expression = measure.sqlExpression(fp, DefaultQueryRewriter.INSTANCE, true);
    Assertions.assertThat(expression)
            .isEqualTo("sum(case when (`recommendation`.`finalprice` > `recommendation`.`recoprice` and `recommendation`.`recoprice` > '0')" +
                    " then (`recommendation`.`finalprice`-`recommendation`.`recoprice`) end)" +
                    " as `increase_sum`");

    Field initial_price = new TableField("recommendation.initial_price");
    Field one = new ConstantField(1);
    Field tvaRate = new TableField("product.tva_rate");
    Measure whatever = sum("whatever", divide(initial_price, plus(one, tvaRate)));
    Assertions.assertThat(whatever.sqlExpression(fp, DefaultQueryRewriter.INSTANCE, true))
            .isEqualTo("sum((`recommendation`.`initial_price`/(1+`product`.`tva_rate`))) as `whatever`");
  }
}
