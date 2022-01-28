package me.paulbares.query;

import me.paulbares.query.dto.JoinDto;
import me.paulbares.query.dto.JoinMappingDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.TableDto;
import me.paulbares.query.context.Totals;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TestSQLTranslator {

  private static final String BASE_STORE_NAME = "baseStore";

  @Test
  void testGrandTotal() {
    QueryDto query = new QueryDto()
            .aggregatedMeasure("pnl", "sum")
            .aggregatedMeasure("delta", "sum")
            .aggregatedMeasure("pnl", "avg")
            .table(BASE_STORE_NAME);

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select sum(`pnl`), sum(`delta`), avg(`pnl`) from " + BASE_STORE_NAME);
  }

  @Test
  void testGroupBy() {
    QueryDto query = new QueryDto()
            .wildcardCoordinate("scenario")
            .wildcardCoordinate("type")
            .aggregatedMeasure("pnl", "sum")
            .aggregatedMeasure("delta", "sum")
            .aggregatedMeasure("pnl", "avg")
            .table(BASE_STORE_NAME);

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select `scenario`, `type`, sum(`pnl`), sum(`delta`), avg(`pnl`) from " + BASE_STORE_NAME + " group by " +
                    "`scenario`, `type`");
  }

  @Test
  void testSingleConditionSingleField() {
    QueryDto query = new QueryDto()
            .coordinate("scenario", "Base")
            .wildcardCoordinate("type")
            .aggregatedMeasure("pnl", "sum")
            .aggregatedMeasure("delta", "sum")
            .aggregatedMeasure("pnl", "avg")
            .table(BASE_STORE_NAME);

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select `scenario`, `type`, sum(`pnl`), sum(`delta`), avg(`pnl`) from " + BASE_STORE_NAME + " where " +
                    "`scenario` = 'Base' group by `scenario`, `type`");
  }

  @Test
  void testConditionsSeveralField() {
    QueryDto query = new QueryDto()
            .coordinate("scenario", "Base")
            .coordinates("type", "A", "B")
            .aggregatedMeasure("pnl", "sum")
            .aggregatedMeasure("delta", "sum")
            .aggregatedMeasure("pnl", "avg")
            .table(BASE_STORE_NAME);

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select `scenario`, `type`, sum(`pnl`), sum(`delta`), avg(`pnl`) from " + BASE_STORE_NAME + " where `scenario` = 'Base' and `type` in ('A', 'B') group by `scenario`, `type`");
  }

  @Test
  void testDifferentMeasures() {
    QueryDto query = new QueryDto()
            .table(BASE_STORE_NAME)
            .aggregatedMeasure("pnl", "sum")
            .expressionMeasure("indice", "100 * sum(`delta`) / sum(`pnl`)");

    Assertions.assertThat(SQLTranslator.translate(query))
          .isEqualTo("select sum(`pnl`), 100 * sum(`delta`) / sum(`pnl`) as `indice` from " + BASE_STORE_NAME);
  }

  @Test
  void testWithTotalsTop() {
    QueryDto query = new QueryDto()
            .wildcardCoordinate("scenario")
            .aggregatedMeasure("price", "sum")
            .context(Totals.KEY, Totals.VISIBLE_TOP)
            .table(BASE_STORE_NAME);

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select `scenario`, sum(`price`) from " + BASE_STORE_NAME + " group by rollup(`scenario`) " +
                    "order by case when `scenario` is null then 0 else 1 end, `scenario`  asc");
  }

  @Test
  void testWithTotalsBottom() {
    QueryDto query = new QueryDto()
            .wildcardCoordinate("scenario")
            .aggregatedMeasure("price", "sum")
            .context(Totals.KEY, Totals.VISIBLE_BOTTOM)
            .table(BASE_STORE_NAME);

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select `scenario`, sum(`price`) from " + BASE_STORE_NAME + " group by rollup(`scenario`) " +
                    "order by case when `scenario` is null then 1 else 0 end, `scenario`  asc");
  }

  @Test
  void testJoins() {
    TableDto baseStore = new TableDto(BASE_STORE_NAME);
    TableDto table1 = new TableDto("table1");
    JoinMappingDto mappingBaseToTable1 = new JoinMappingDto("id", "table1_id");
    TableDto table2 = new TableDto("table2");
    JoinMappingDto mappingBaseToTable2 = new JoinMappingDto("id", "table2_id");
    TableDto table3 = new TableDto("table3");
    JoinMappingDto mappingTable2ToTable3 = new JoinMappingDto("table2_field_1", "table3_id");
    TableDto table4 = new TableDto("table4");
    List<JoinMappingDto> mappingTable1ToTable4 = List.of(
            new JoinMappingDto("table1_field_2", "table4_id_1"),
            new JoinMappingDto("table1_field_3", "table4_id_2"));

    baseStore.joins.add(new JoinDto(table1, "inner", mappingBaseToTable1));
    baseStore.joins.add(new JoinDto(table2, "left", mappingBaseToTable2));

    table1.joins.add(new JoinDto(table4, "inner", mappingTable1ToTable4));
    table2.joins.add(new JoinDto(table3, "inner", mappingTable2ToTable3));

    QueryDto query = new QueryDto()
            .table(baseStore)
            .aggregatedMeasure("pnl", "avg");

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select avg(`pnl`) from " + BASE_STORE_NAME
                    + " inner join table1 on " + BASE_STORE_NAME + ".id = table1.table1_id"
                    + " inner join table4 on table1.table1_field_2 = table4.table4_id_1 and table1.table1_field_3 = table4.table4_id_2"
                    + " left join table2 on " + BASE_STORE_NAME + ".id = table2.table2_id"
                    + " inner join table3 on table2.table2_field_1 = table3.table3_id"
            );
  }
}
