package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.context.Totals;
import me.paulbares.store.Field;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static me.paulbares.SparkDatastore.MAIN_SCENARIO_NAME;
import static me.paulbares.query.QueryEngine.GRAND_TOTAL;
import static me.paulbares.query.QueryEngine.TOTAL;

public class TestQueryEngine {

  static SparkDatastore ds;

  @BeforeAll
  static void setup() {
    Field ean = new Field("ean", String.class);
    Field category = new Field("category", String.class);
    Field price = new Field("price", double.class);
    Field qty = new Field("quantity", int.class);
    ds = new SparkDatastore(List.of(ean, category, price, qty));

    ds.load(MAIN_SCENARIO_NAME, List.of(
            new Object[]{"bottle", "drink", 2d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    ds.load("s1", List.of(
            new Object[]{"bottle", "drink", 4d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    ds.load("s2", List.of(
            new Object[]{"bottle", "drink", 1.5d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));
  }

  @Test
  void testQueryWildcard() {
    Query query = new Query()
            .addWildcardCoordinate("scenario")
            .addAggregatedMeasure("price", "sum")
            .addAggregatedMeasure("quantity", "sum");
    List<Row> collect = new SparkQueryEngine(ds).executeSpark(query).collectAsList();
    Assertions.assertThat(collect).containsExactlyInAnyOrder(
            RowFactory.create("base", 15.0d, 33),
            RowFactory.create("s1", 17.0d, 33),
            RowFactory.create("s2", 14.5d, 33));
  }

  @Test
  void testQueryWildcardWithTotals() {
    Query query = new Query()
            .addWildcardCoordinate("scenario")
            .addAggregatedMeasure("price", "sum")
            .addAggregatedMeasure("quantity", "sum")
            .addContext(Totals.KEY, Totals.VISIBLE_TOP);
    List<Row> collect = new SparkQueryEngine(ds).executeSpark(query).collectAsList();
    Assertions.assertThat(collect).containsExactly(
            RowFactory.create(GRAND_TOTAL, 15.d + 17.d + 14.5, 33 * 3),
            RowFactory.create("base", 15.0d, 33),
            RowFactory.create("s1", 17.0d, 33),
            RowFactory.create("s2", 14.5d, 33));
  }

  @Test
  void testQueryWildcardAndCrossjoinWithTotals() {
    Query query = new Query()
            .addWildcardCoordinate("scenario")
            .addWildcardCoordinate("category")
            .addWildcardCoordinate("ean")
            .addAggregatedMeasure("price", "sum")
            .addAggregatedMeasure("quantity", "sum")
            .addContext(Totals.KEY, Totals.VISIBLE_TOP);

    Dataset<Row> dataset = new SparkQueryEngine(ds).executeSpark(query);
    List<Row> collect = dataset.collectAsList();
    Assertions.assertThat(collect).containsExactly(
            RowFactory.create(GRAND_TOTAL, null, null, 15.d + 17.d + 14.5d, 33 * 3),
            RowFactory.create("base", TOTAL, null,  15.0d, 33),
            RowFactory.create("base", "cloth", TOTAL, 10.0d, 3),
            RowFactory.create("base", "cloth", "shirt", 10.0d, 3),
            RowFactory.create("base", "drink", TOTAL, 2.0d, 10),
            RowFactory.create("base", "drink", "bottle", 2.0d, 10),
            RowFactory.create("base", "food", TOTAL, 3.0d, 20),
            RowFactory.create("base", "food", "cookie", 3.0d, 20),

            RowFactory.create("s1", TOTAL, null,  17.0d, 33),
            RowFactory.create("s1", "cloth", TOTAL, 10.0d, 3),
            RowFactory.create("s1", "cloth", "shirt", 10.0d, 3),
            RowFactory.create("s1", "drink", TOTAL, 4.0d, 10),
            RowFactory.create("s1", "drink", "bottle", 4.0d, 10),
            RowFactory.create("s1", "food", TOTAL, 3.0d, 20),
            RowFactory.create("s1", "food", "cookie", 3.0d, 20),

            RowFactory.create("s2", TOTAL, null,  14.5d, 33),
            RowFactory.create("s2", "cloth", TOTAL, 10.0d, 3),
            RowFactory.create("s2", "cloth", "shirt", 10.0d, 3),
            RowFactory.create("s2", "drink", TOTAL, 1.5d, 10),
            RowFactory.create("s2", "drink", "bottle", 1.5d, 10),
            RowFactory.create("s2", "food", TOTAL, 3.0d, 20),
            RowFactory.create("s2", "food", "cookie", 3.0d, 20));
  }

  @Test
  void testQueryWildcardAndCrossjoinWithTotalsPositionBottom() {
    Query query = new Query()
            .addWildcardCoordinate("scenario")
            .addWildcardCoordinate("category")
            .addWildcardCoordinate("ean")
            .addAggregatedMeasure("price", "sum")
            .addAggregatedMeasure("quantity", "sum")
            .addContext(Totals.KEY, Totals.VISIBLE_BOTTOM);

    Dataset<Row> dataset = new SparkQueryEngine(ds).executeSpark(query);
    List<Row> collect = dataset.collectAsList();
    Assertions.assertThat(collect).containsExactly(
            RowFactory.create("base", "cloth", "shirt", 10.0d, 3),
            RowFactory.create("base", "cloth", TOTAL, 10.0d, 3),
            RowFactory.create("base", "drink", "bottle", 2.0d, 10),
            RowFactory.create("base", "drink", TOTAL, 2.0d, 10),
            RowFactory.create("base", "food", "cookie", 3.0d, 20),
            RowFactory.create("base", "food", TOTAL, 3.0d, 20),
            RowFactory.create("base", TOTAL, null,  15.0d, 33),

            RowFactory.create("s1", "cloth", "shirt", 10.0d, 3),
            RowFactory.create("s1", "cloth", TOTAL, 10.0d, 3),
            RowFactory.create("s1", "drink", "bottle", 4.0d, 10),
            RowFactory.create("s1", "drink", TOTAL, 4.0d, 10),
            RowFactory.create("s1", "food", "cookie", 3.0d, 20),
            RowFactory.create("s1", "food", TOTAL, 3.0d, 20),
            RowFactory.create("s1", TOTAL, null,  17.0d, 33),

            RowFactory.create("s2", "cloth", "shirt", 10.0d, 3),
            RowFactory.create("s2", "cloth", TOTAL, 10.0d, 3),
            RowFactory.create("s2", "drink", "bottle", 1.5d, 10),
            RowFactory.create("s2", "drink", TOTAL, 1.5d, 10),
            RowFactory.create("s2", "food", "cookie", 3.0d, 20),
            RowFactory.create("s2", "food", TOTAL, 3.0d, 20),
            RowFactory.create("s2", TOTAL, null,  14.5d, 33),
            RowFactory.create(GRAND_TOTAL, null, null, 15.d + 17.d + 14.5d, 33 * 3));
  }

  @Test
  void testQuerySeveralCoordinates() {
    Query query = new Query()
            .addCoordinates("scenario", "s1", "s2")
            .addAggregatedMeasure("price", "sum")
            .addAggregatedMeasure("quantity", "sum");
    List<Row> collect = new SparkQueryEngine(ds).executeSpark(query).collectAsList();
    Assertions.assertThat(collect).containsExactlyInAnyOrder(
            RowFactory.create("s1", 17.0d, 33),
            RowFactory.create("s2", 14.5d, 33));
  }

  @Test
  void testQuerySingleCoordinate() {
    Query query = new Query()
            .addSingleCoordinate("scenario", "s1")
            .addAggregatedMeasure("price", "sum")
            .addAggregatedMeasure("quantity", "sum");
    Dataset<Row> rowDataset = new SparkQueryEngine(ds).executeSpark(query);
    List<Row> collect = rowDataset.collectAsList();
    Assertions.assertThat(collect).containsExactlyInAnyOrder(
            RowFactory.create("s1", 17.0d, 33));
  }

  /**
   * Without measure, we can use it to do a discovery.
   */
  @Test
  void testDiscovery() {
    Query query = new Query().addWildcardCoordinate("scenario");
    Dataset<Row> rowDataset = new SparkQueryEngine(ds).executeSpark(query);
    List<Row> collect = rowDataset.collectAsList();
    Assertions.assertThat(collect)
            .containsExactlyInAnyOrder(
                    RowFactory.create(MAIN_SCENARIO_NAME),
                    RowFactory.create("s1"),
                    RowFactory.create("s2")
            );
  }

  @Test
  void testJsonConverter() throws Exception {
    Query query = new Query()
            .addWildcardCoordinate("scenario")
            .addAggregatedMeasure("price", "sum")
            .addAggregatedMeasure("quantity", "sum");
    Table dataset = new SparkQueryEngine(ds).execute(query);
    String actual = JacksonUtil.tableToCsv(dataset);
    Map map = JacksonUtil.mapper.readValue(actual, Map.class);
    Assertions.assertThat((List) map.get("columns"))
            .containsExactly("scenario", "sum(price)", "sum(quantity)");
    Assertions.assertThat((List) map.get("rows"))
            .containsExactlyInAnyOrder(
                    List.of("base", 15d, 33),
                    List.of("s1", 17d, 33),
                    List.of("s2", 14.5d, 33));
  }
}
