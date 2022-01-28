package me.paulbares.query;

import me.paulbares.query.dto.QueryDto;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.context.Totals;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static me.paulbares.query.QueryEngine.GRAND_TOTAL;
import static me.paulbares.query.QueryEngine.TOTAL;
import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestQueryEngine {

  protected Datastore datastore;

  protected QueryEngine queryEngine;

  protected String storeName = "storeName";

  protected abstract QueryEngine createQueryEngine(Datastore datastore);

  protected abstract Datastore createDatastore(String storeName, List<Field> fields);

  @BeforeAll
  void setup() {
    Field ean = new Field("ean", String.class);
    Field category = new Field("category", String.class);
    Field price = new Field("price", double.class);
    Field qty = new Field("quantity", int.class);

    this.datastore = createDatastore(this.storeName, List.of(ean, category, price, qty));
    this.queryEngine = createQueryEngine(this.datastore);

    this.datastore.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"bottle", "drink", 2d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    this.datastore.load("s1", this.storeName, List.of(
            new Object[]{"bottle", "drink", 4d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    this.datastore.load("s2", this.storeName, List.of(
            new Object[]{"bottle", "drink", 1.5d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));
  }

  @Test
  void testQueryWildcard() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate("scenario")
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum");
    Table result = this.queryEngine.execute(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("base", 15.0d, 33l),
            List.of("s1", 17.0d, 33l),
            List.of("s2", 14.5d, 33l));
  }

  @Test
  void testQueryWildcardCount() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate("scenario")
            .aggregatedMeasure("*", "count");
    Table result = this.queryEngine.execute(query);
    result.show();
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("base", 3l),
            List.of("s1", 3l),
            List.of("s2", 3l));
  }

  @Test
  void testQueryWildcardWithTotals() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate("scenario")
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum")
            .context(Totals.KEY, Totals.VISIBLE_TOP);
    Table table = this.queryEngine.execute(query);
    Assertions.assertThat(table).containsExactly(
            List.of(GRAND_TOTAL, 15.d + 17.d + 14.5, 33 * 3l),
            List.of("base", 15.0d, 33l),
            List.of("s1", 17.0d, 33l),
            List.of("s2", 14.5d, 33l));
  }

  @Test
  void testQueryWildcardAndCrossjoinWithTotals() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate("scenario")
            .wildcardCoordinate("category")
            .wildcardCoordinate("ean")
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum")
            .context(Totals.KEY, Totals.VISIBLE_TOP);

    Table table = this.queryEngine.execute(query);
    Assertions.assertThat(table).containsExactly(
            Arrays.asList(GRAND_TOTAL, null, null, 15.d + 17.d + 14.5d, 33 * 3l),
            Arrays.asList("base", TOTAL, null,  15.0d, 33l),
            Arrays.asList("base", "cloth", TOTAL, 10.0d, 3l),
            Arrays.asList("base", "cloth", "shirt", 10.0d, 3l),
            Arrays.asList("base", "drink", TOTAL, 2.0d, 10l),
            Arrays.asList("base", "drink", "bottle", 2.0d, 10l),
            Arrays.asList("base", "food", TOTAL, 3.0d, 20l),
            Arrays.asList("base", "food", "cookie", 3.0d, 20l),

            Arrays.asList("s1", TOTAL, null,  17.0d, 33l),
            Arrays.asList("s1", "cloth", TOTAL, 10.0d, 3l),
            Arrays.asList("s1", "cloth", "shirt", 10.0d, 3l),
            Arrays.asList("s1", "drink", TOTAL, 4.0d, 10l),
            Arrays.asList("s1", "drink", "bottle", 4.0d, 10l),
            Arrays.asList("s1", "food", TOTAL, 3.0d, 20l),
            Arrays.asList("s1", "food", "cookie", 3.0d, 20l),

            Arrays.asList("s2", TOTAL, null,  14.5d, 33l),
            Arrays.asList("s2", "cloth", TOTAL, 10.0d, 3l),
            Arrays.asList("s2", "cloth", "shirt", 10.0d, 3l),
            Arrays.asList("s2", "drink", TOTAL, 1.5d, 10l),
            Arrays.asList("s2", "drink", "bottle", 1.5d, 10l),
            Arrays.asList("s2", "food", TOTAL, 3.0d, 20l),
            Arrays.asList("s2", "food", "cookie", 3.0d, 20l));
  }

  @Test
  void testQueryWildcardAndCrossjoinWithTotalsPositionBottom() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate("scenario")
            .wildcardCoordinate("category")
            .wildcardCoordinate("ean")
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum")
            .context(Totals.KEY, Totals.VISIBLE_BOTTOM);
    Table table = this.queryEngine.execute(query);
    Assertions.assertThat(table).containsExactly(
            Arrays.asList("base", "cloth", "shirt", 10.0d, 3l),
            Arrays.asList("base", "cloth", TOTAL, 10.0d, 3l),
            Arrays.asList("base", "drink", "bottle", 2.0d, 10l),
            Arrays.asList("base", "drink", TOTAL, 2.0d, 10l),
            Arrays.asList("base", "food", "cookie", 3.0d, 20l),
            Arrays.asList("base", "food", TOTAL, 3.0d, 20l),
            Arrays.asList("base", TOTAL, null,  15.0d, 33l),

            Arrays.asList("s1", "cloth", "shirt", 10.0d, 3l),
            Arrays.asList("s1", "cloth", TOTAL, 10.0d, 3l),
            Arrays.asList("s1", "drink", "bottle", 4.0d, 10l),
            Arrays.asList("s1", "drink", TOTAL, 4.0d, 10l),
            Arrays.asList("s1", "food", "cookie", 3.0d, 20l),
            Arrays.asList("s1", "food", TOTAL, 3.0d, 20l),
            Arrays.asList("s1", TOTAL, null,  17.0d, 33l),

            Arrays.asList("s2", "cloth", "shirt", 10.0d, 3l),
            Arrays.asList("s2", "cloth", TOTAL, 10.0d, 3l),
            Arrays.asList("s2", "drink", "bottle", 1.5d, 10l),
            Arrays.asList("s2", "drink", TOTAL, 1.5d, 10l),
            Arrays.asList("s2", "food", "cookie", 3.0d, 20l),
            Arrays.asList("s2", "food", TOTAL, 3.0d, 20l),
            Arrays.asList("s2", TOTAL, null,  14.5d, 33l),
            Arrays.asList(GRAND_TOTAL, null, null, 15.d + 17.d + 14.5d, 33 * 3l));
  }

  @Test
  void testQuerySeveralCoordinates() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .coordinates("scenario", "s1", "s2")
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum");
    Table table = this.queryEngine.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(
            List.of("s1", 17.0d, 33l),
            List.of("s2", 14.5d, 33l));
  }

  @Test
  void testQuerySingleCoordinate() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .coordinate("scenario", "s1")
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum");
    Table table = this.queryEngine.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(List.of("s1", 17.0d, 33l));
  }

  /**
   * Without measure, we can use it to do a discovery.
   */
  @Test
  void testDiscovery() {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate("scenario");
    Table table = this.queryEngine.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(
                    List.of(MAIN_SCENARIO_NAME),
                    List.of("s1"),
                    List.of("s2"));
  }

  @Test
  void testJsonConverter() throws Exception {
    QueryDto query = new QueryDto()
            .table(this.storeName)
            .wildcardCoordinate("scenario")
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum");
    Table table = this.queryEngine.execute(query);
    String actual = JacksonUtil.tableToCsv(table);
    Map map = JacksonUtil.mapper.readValue(actual, Map.class);
    Assertions.assertThat((List) map.get("columns")).containsExactly("scenario", "sum(price)", "sum(quantity)");
    Assertions.assertThat((List) map.get("rows")).containsExactlyInAnyOrder(
                    List.of("base", 15d, 33),
                    List.of("s1", 17d, 33),
                    List.of("s2", 14.5d, 33));
  }
}
