package io.squashql.query;

import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.JoinDto;
import io.squashql.query.dto.JoinMappingDto;
import io.squashql.query.dto.TableDto;
import io.squashql.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;

import static io.squashql.query.Functions.*;
import static io.squashql.query.dto.JoinType.INNER;
import static io.squashql.query.dto.JoinType.LEFT;
import static io.squashql.transaction.TransactionManager.SCENARIO_FIELD_NAME;

@Disabled
public class TestSQLTranslator {

  private static final String BASE_STORE_NAME = "baseStore";
  private static final String BASE_STORE_NAME_ESCAPED = SqlUtils.backtickEscape(BASE_STORE_NAME);

  private static final Function<String, Field> fieldProvider = s -> switch (s) {
    case "pnl" -> new Field(BASE_STORE_NAME, s, double.class);
    case "delta" -> new Field(BASE_STORE_NAME, s, Double.class);
    default -> new Field(BASE_STORE_NAME, s, String.class);
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
            .isEqualTo("select `scenario`, `type`, grouping(`scenario`) as `___grouping___scenario___`, grouping(`type`) as `___grouping___type___`, sum(`price`) as `pnl.sum`" +
                    " from " + BASE_STORE_NAME_ESCAPED + " group by rollup(`scenario`, `type`)");
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
            .isEqualTo("select `scenario`, `type`, grouping(`scenario`) as `___grouping___scenario___`, sum(`price`) as `pnl.sum`" +
                    " from " + BASE_STORE_NAME_ESCAPED + " group by `type`, rollup(`scenario`)");
  }

  @Test
  void testJoins() {
    TableDto baseStore = new TableDto(BASE_STORE_NAME);
    TableDto table1 = new TableDto("table1");
    JoinMappingDto mappingBaseToTable1 = new JoinMappingDto(baseStore.name, "id", table1.name, "table1_id");
    TableDto table2 = new TableDto("table2");
    JoinMappingDto mappingBaseToTable2 = new JoinMappingDto(baseStore.name, "id", table2.name, "table2_id");
    TableDto table3 = new TableDto("table3");
    JoinMappingDto mappingTable2ToTable3 = new JoinMappingDto(table2.name, "table2_field_1", table3.name, "table3_id");
    TableDto table4 = new TableDto("table4");
    List<JoinMappingDto> mappingTable1ToTable4 = List.of(
            new JoinMappingDto(table1.name, "table1_field_2", table4.name, "table4_id_1"),
            new JoinMappingDto(table1.name, "table1_field_3", table4.name, "table4_id_2"));

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
    JoinMappingDto jAToB = new JoinMappingDto(a.name, "a_id", b.name, "b_id");
    TableDto c = new TableDto("C");
    // should be able to ref. both column of A and B in the join.
    List<JoinMappingDto> jCToAB = List.of(
            new JoinMappingDto(c.name, "c_other_id", b.name, "b_other_id"),
            new JoinMappingDto(c.name, "c_f", a.name, "a_f"));

    a.join(b, INNER, jAToB);
    a.join(c, LEFT, jCToAB);

    DatabaseQuery query = new DatabaseQuery().table(a).withSelect(fieldProvider.apply("c.y"));

    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
            .isEqualTo("select `c.y` from `A` " +
                    "inner join `B` on `A`.`a_id` = `B`.`b_id` " +
                    "left join `C` on `C`.`c_other_id` = `B`.`b_other_id` and `C`.`c_f` = `A`.`a_f` " +
                    "group by `c.y`");
  }

  @Test
  void testConditionsWithValue() {
    DatabaseQuery query = new DatabaseQuery()
            .withSelect(fieldProvider.apply(SCENARIO_FIELD_NAME))
            .withSelect(fieldProvider.apply("type"))
            .aggregatedMeasure("pnl.sum", "pnl", "sum")
            .whereCriteria(all(
                    criterion(SCENARIO_FIELD_NAME, and(eq("base"), eq("s1"), eq("s2"))),
                    criterion("delta", ge(123d)),
                    criterion("type", or(eq("A"), eq("B"))),
                    criterion("pnl", lt(10d))))
            .table(BASE_STORE_NAME);
    Assertions.assertThat(SQLTranslator.translate(query, fieldProvider))
            .isEqualTo("select `scenario`, `type`, sum(`pnl`) as `pnl.sum` from " + BASE_STORE_NAME_ESCAPED
                    + " where (`scenario` = 'base' and `scenario` = 's1' and `scenario` = 's2'"
                    + " and `delta` >= 123.0 and `type` = 'A' or `type` = 'B' and `pnl` < 10.0)"
                    + " group by `scenario`, `type`"
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
}
