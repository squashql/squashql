package io.squashql.query;

import io.squashql.query.database.AQueryEngine;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.*;
import io.squashql.store.Field;
import io.squashql.store.Store;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.squashql.query.Functions.*;
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
  private static final Function<String, Field> fieldProvider = s -> {
    Function<String, Class<?>> type = f -> switch (f) {
      case "pnl", BASE_STORE_NAME + "." + "pnl" -> double.class;
      case "delta", BASE_STORE_NAME + "." + "delta" -> Double.class;
      default -> String.class;
    };
    String[] split = s.split("\\.");
    if (split.length > 1) {
      String tableName = split[0];
      String fieldNameInTable = split[1];
      return new Field(tableName, fieldNameInTable, type.apply(fieldNameInTable));
    } else {
      return new Field(null, s, type.apply(s));
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

    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
            .isEqualTo("select sum(`pnl`) as `pnl.sum`, sum(`delta`) as `delta.sum`, avg(`pnl`) as `pnl.avg`, avg(`pnl`) as `mean pnl` from " + BASE_STORE_NAME_ESCAPED);
  }

  @Test
  void testLimit() {
    DatabaseQuery query = new DatabaseQuery()
            .aggregatedMeasure("pnl.sum", "pnl", "sum")
            .limit(8)
            .table(BASE_STORE_NAME);

    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
            .isEqualTo("select sum(`pnl`) as `pnl.sum` from `" + BASE_STORE_NAME + "` limit 8");
  }

  @Test
  void testGroupBy() {
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(fieldProvider.apply(SCENARIO_FIELD_NAME))
            .withSelect(fieldProvider.apply("type"))
            .aggregatedMeasure("pnl.sum", "pnl", "sum")
            .aggregatedMeasure("delta.sum", "delta", "sum")
            .aggregatedMeasure("pnl.avg", "pnl", "avg")
            .table(BASE_STORE_NAME);

    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
            .isEqualTo("select `scenario`, `type`, sum(`pnl`) as `pnl.sum`, sum(`delta`) as `delta.sum`, avg(`pnl`) as `pnl.avg` from " + BASE_STORE_NAME_ESCAPED + " group by " +
                    "`scenario`, `type`");
  }

  @Test
  void testGroupByWithFullName() {
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(fieldProvider.apply(SqlUtils.getFieldFullName(BASE_STORE_NAME, SCENARIO_FIELD_NAME)))
            .withSelect(fieldProvider.apply(SqlUtils.getFieldFullName(BASE_STORE_NAME, "type")))
            .aggregatedMeasure("pnl.sum", SqlUtils.getFieldFullName(BASE_STORE_NAME, "pnl"), "sum")
            .aggregatedMeasure("delta.sum", SqlUtils.getFieldFullName(BASE_STORE_NAME, "delta"), "sum")
            .table(BASE_STORE_NAME);

    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
            .isEqualTo("select `baseStore`.`scenario`, `baseStore`.`type`, sum(`baseStore`.`pnl`) as `pnl.sum`, sum(`baseStore`.`delta`) as `delta.sum` from " + BASE_STORE_NAME_ESCAPED + " group by " +
                    "`baseStore`.`scenario`, `baseStore`.`type`");
  }

  @Test
  void testDifferentMeasures() {
    DatabaseQuery query = new DatabaseQuery()
            .table(BASE_STORE_NAME)
            .aggregatedMeasure("pnl.sum", "pnl", "sum")
            .expressionMeasure("indice", "100 * sum(`delta`) / sum(`pnl`)");

    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
            .isEqualTo("select sum(`pnl`) as `pnl.sum`, 100 * sum(`delta`) / sum(`pnl`) as `indice` from " + BASE_STORE_NAME_ESCAPED);
  }

  @Test
  void testWithFullRollup() {
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(fieldProvider.apply(SCENARIO_FIELD_NAME))
            .withSelect(fieldProvider.apply("type"))
            .withRollup(fieldProvider.apply(SCENARIO_FIELD_NAME))
            .withRollup(fieldProvider.apply("type"))
            .aggregatedMeasure("pnl.sum", "price", "sum")
            .table(BASE_STORE_NAME);

    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
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
            .withSelect(fieldProvider.apply(SqlUtils.getFieldFullName(BASE_STORE_NAME, SCENARIO_FIELD_NAME)))
            .withSelect(fieldProvider.apply(SqlUtils.getFieldFullName(BASE_STORE_NAME, "type")))
            .withRollup(fieldProvider.apply(SqlUtils.getFieldFullName(BASE_STORE_NAME, SCENARIO_FIELD_NAME)))
            .withRollup(fieldProvider.apply(SqlUtils.getFieldFullName(BASE_STORE_NAME, "type")))
            .aggregatedMeasure("pnl.sum", "price", "sum")
            .table(BASE_STORE_NAME);

    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
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
            .withSelect(fieldProvider.apply(SCENARIO_FIELD_NAME))
            .withSelect(fieldProvider.apply("type"))
            .withRollup(fieldProvider.apply(SCENARIO_FIELD_NAME))
            .aggregatedMeasure("pnl.sum", "price", "sum")
            .table(BASE_STORE_NAME);

    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
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

    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
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

    Function<String, Field> fieldSupplier = AQueryEngine.createFieldSupplier(Map.of(
            "A", new Store("A", List.of(new Field("A", "a_id", int.class), new Field("A", "a_f", int.class), new Field("A", "y", int.class))),
            "B", new Store("B", List.of(new Field("B", "b_id", int.class), new Field("B", "b_other_id", int.class))),
            "C", new Store("C", List.of(new Field("c", "a_id", int.class), new Field("C", "c_f", int.class), new Field("C", "c_other_id", int.class)))
    ));

    DatabaseQuery query = new DatabaseQuery().table(a).withSelect(fieldSupplier.apply("A.y"));

    Assertions.assertThat(SQLTranslator.translate(query, fieldSupplier))
            .isEqualTo("select `A`.`y` from `A` " +
                    "inner join `B` on `A`.`a_id` = `B`.`b_id` " +
                    "left join `C` on `C`.`c_other_id` = `B`.`b_other_id` and `C`.`c_f` = `A`.`a_f` " +
                    "group by `A`.`y`");
  }

  @Test
  void testConditionsWithValue() {
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(fieldProvider.apply(SCENARIO_FIELD_NAME))
            .withSelect(fieldProvider.apply("type"))
            .aggregatedMeasure("pnl.sum", "pnl", "sum")
            .whereCriteria(all(
                    criterion(SCENARIO_FIELD_NAME, or(eq("base"), eq("s1"), eq("s2"))),
                    criterion("delta", ge(123d)),
                    criterion("type", or(eq("A"), eq("B"))),
                    criterion("pnl", lt(10d)))
            )
            .table(BASE_STORE_NAME);
    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
            .isEqualTo("select `scenario`, `type`, sum(`pnl`) as `pnl.sum` from " + BASE_STORE_NAME_ESCAPED
                    + " where (((`scenario` = 'base' or `scenario` = 's1') or `scenario` = 's2')"
                    + " and `delta` >= 123.0 and (`type` = 'A' or `type` = 'B') and `pnl` < 10.0)"
                    + " group by `scenario`, `type`"
            );
  }

  @Test
  void testConditionWithValueFullPath() {
    Field field = new Field(BASE_STORE_NAME, SCENARIO_FIELD_NAME, String.class);
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(field)
            .aggregatedMeasure("pnl.sum", "pnl", "sum")
            .whereCriteria(criterion(SqlUtils.getFieldFullName(field), and(eq("base"), eq("s2"))))
            .table(BASE_STORE_NAME);
    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
            .isEqualTo("select `baseStore`.`scenario`, sum(`pnl`) as `pnl.sum` from " + BASE_STORE_NAME_ESCAPED
                    + " where (`baseStore`.`scenario` = 'base' and `baseStore`.`scenario` = 's2')"
                    + " group by `baseStore`.`scenario`"
            );
  }

  @Test
  void testSelectFromSelect() {
    // Kind of leaf agg. !!!
    TableDto a = new TableDto("a");
    DatabaseQuery subQuery = new DatabaseQuery()
            .table(a)
            .withSelect(fieldProvider.apply("c1"))
            .withSelect(fieldProvider.apply("c3"))
            .withMeasure(avg("mean", "c2"));

    DatabaseQuery query = new DatabaseQuery()
            .subQuery(subQuery)
            .withSelect(fieldProvider.apply("c3")) // c3 needs to be in the subquery
            .withMeasure(sum("sum GT", "mean"))
            .whereCriteria(criterion("type", eq("myType")));
    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
            .isEqualTo("select `c3`, sum(`mean`) as `sum GT` from (select `c1`, `c3`, avg(`c2`) as `mean` from `a` group by `c1`, `c3`) where `type` = 'myType' group by `c3`");
  }

  @Test
  void testBinaryOperationMeasure() {
    TableDto a = new TableDto("a");
    DatabaseQuery query = new DatabaseQuery()
            .table(a)
            .withMeasure(plus("plus", sum("pnl.sum", "pnl"), avg("delta.avg", "delta")));
    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
            .isEqualTo("select sum(`pnl`)+avg(`delta`) as `plus` from `a`");
  }

  @Test
  void testAggregatedMeasures() {
    TableDto a = new TableDto(BASE_STORE_NAME);
    DatabaseQuery query = new DatabaseQuery()
            .table(a)
            .withMeasure(sum("pnlSum", "pnl"))
            .withMeasure(sumIf("pnlSumFiltered", "pnl", criterion("country", eq("france"))));
    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
            .isEqualTo("select sum(`pnl`) as `pnlSum`, sum(case when `country` = 'france' then `pnl` end) as `pnlSumFiltered` from `" + BASE_STORE_NAME + "`");

    // With full path
    query = new DatabaseQuery()
            .table(a)
            .withMeasure(sum("pnlSum", a.name + ".pnl"))
            .withMeasure(sumIf("pnlSumFiltered", a.name + ".pnl", criterion(a.name + ".country", eq("france"))));
    String format = "select sum(`%1$s`.`pnl`) as `pnlSum`, sum(case when `%1$s`.`country` = 'france' then `%1$s`.`pnl` end) as `pnlSumFiltered` from `%1$s`";
    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
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
            .withSelect(fieldProvider.apply("id"))
            .withSelect(fieldProvider.apply("b"));
    String expected = String.format("with %2$s as (select 0 as `a`, '0' as `b` union all select 1 as `a`, '1' as `b`) " +
                    "select `id`, `b`, sum(`pnl`) as `pnl.sum` from `%1$s` " +
                    "inner join %2$s on `id` = `a` group by `id`, `b`",
            BASE_STORE_NAME, virtualTable.name);
    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
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
            .withSelect(fieldProvider.apply("id"))
            .withSelect(fieldProvider.apply("b"));
    String expected = String.format("with %2$s as (select 0 as `a`, '0' as `b` union all select 1 as `a`, '1' as `b`) " +
                    "select `id`, `b`, sum(`pnl`) as `pnl.sum` from `%1$s` " +
                    "inner join %2$s on `%1$s`.`id` = %2$s.`a` group by `id`, `b`",
            BASE_STORE_NAME, virtualTable.name);
    // IMPORTANT: No backtick in the join condition for the field belonging to the virtual table.
    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
            .isEqualTo(expected);
  }
}
