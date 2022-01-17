package me.paulbares.query;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static me.paulbares.SparkDatastore.BASE_STORE_NAME;

public class TestSQLTranslator {

  @Test
  void testGrandTotal() {
    Query query = new Query()
            .addAggregatedMeasure("pnl", "sum")
            .addAggregatedMeasure("delta", "sum")
            .addAggregatedMeasure("pnl", "avg");

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select sum(`pnl`), sum(`delta`), avg(`pnl`) from " + BASE_STORE_NAME);
  }

  @Test
  void testGroupBy() {
    Query query = new Query()
            .addWildcardCoordinate("scenario")
            .addWildcardCoordinate("type")
            .addAggregatedMeasure("pnl", "sum")
            .addAggregatedMeasure("delta", "sum")
            .addAggregatedMeasure("pnl", "avg");

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select `scenario`, `type`, sum(`pnl`), sum(`delta`), avg(`pnl`) from " + BASE_STORE_NAME + " group by " +
                    "`scenario`, `type`");
  }

  @Test
  void testSingleConditionSingleField() {
    Query query = new Query()
            .addSingleCoordinate("scenario", "Base")
            .addWildcardCoordinate("type")
            .addAggregatedMeasure("pnl", "sum")
            .addAggregatedMeasure("delta", "sum")
            .addAggregatedMeasure("pnl", "avg");

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select `scenario`, `type`, sum(`pnl`), sum(`delta`), avg(`pnl`) from " + BASE_STORE_NAME + " where " +
                    "`scenario` = 'Base' group by `scenario`, `type`");
  }

  @Test
  void testConditionsSeveralField() {
    Query query = new Query()
            .addSingleCoordinate("scenario", "Base")
            .addCoordinates("type", "A", "B")
            .addAggregatedMeasure("pnl", "sum")
            .addAggregatedMeasure("delta", "sum")
            .addAggregatedMeasure("pnl", "avg");

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select `scenario`, `type`, sum(`pnl`), sum(`delta`), avg(`pnl`) from " + BASE_STORE_NAME + " where `scenario` = 'Base' and `type` in ('A', 'B') group by `scenario`, `type`");
  }

  @Test
  void testDifferentMeasures() {
    Query query = new Query()
            .addAggregatedMeasure("pnl", "sum")
            .addExpressionMeasure("indice", "100 * sum(`delta`) / sum(`pnl`)");

    Assertions.assertThat(SQLTranslator.translate(query))
          .isEqualTo("select sum(`pnl`), 100 * sum(`delta`) / sum(`pnl`) as `indice` from " + BASE_STORE_NAME);
  }

  @Test
  void testWithTotalsTop() {
    Query query = new Query()
            .addWildcardCoordinate("scenario")
            .addAggregatedMeasure("price", "sum")
            .withTotals();

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select `scenario`, sum(`price`) from " + BASE_STORE_NAME + " group by rollup(`scenario`) " +
                    "order by case when `scenario` is null then 0 else 1 end, `scenario`  asc");
  }

  @Test
  void testWithTotalsBottom() {
    Query query = new Query()
            .addWildcardCoordinate("scenario")
            .addAggregatedMeasure("price", "sum")
            .addContext(QueryContext.totalsPosition, QueryContext.totalsPositionBottom)
            .withTotals();

    Assertions.assertThat(SQLTranslator.translate(query))
            .isEqualTo("select `scenario`, sum(`price`) from " + BASE_STORE_NAME + " group by rollup(`scenario`) " +
                    "order by case when `scenario` is null then 1 else 0 end, `scenario`  asc");
  }
}
