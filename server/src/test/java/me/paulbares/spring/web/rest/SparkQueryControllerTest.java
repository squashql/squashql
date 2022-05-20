package me.paulbares.spring.web.rest;

import me.paulbares.client.SimpleTable;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.AggregatedMeasure;
import me.paulbares.query.ExpressionMeasure;
import me.paulbares.query.Measure;
import me.paulbares.query.QueryBuilder;
import me.paulbares.query.QueryEngine;
import me.paulbares.query.UnresolvedExpressionMeasure;
import me.paulbares.query.context.Repository;
import me.paulbares.query.context.Totals;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.ScenarioComparisonDto;
import me.paulbares.query.dto.ScenarioGroupingQueryDto;
import me.paulbares.store.Datastore;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static me.paulbares.query.ScenarioGroupingExecutor.COMPARISON_METHOD_ABS_DIFF;
import static me.paulbares.query.ScenarioGroupingExecutor.REF_POS_PREVIOUS;
import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;
import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
public class SparkQueryControllerTest {

  private static final String REPO_URL = "https://raw.githubusercontent" +
          ".com/paulbares/aitm-assets/main/metrics-controller-test.json";

  @Autowired
  private MockMvc mvc;

  @Test
  public void testQuery() throws Exception {
    var our = QueryBuilder.table("our_prices");
    var their = QueryBuilder.table("their_prices");
    var our_to_their = QueryBuilder.table("our_stores_their_stores");
    our.innerJoin(our_to_their, "pdv", "our_store");
    our_to_their.innerJoin(their, "their_store", "competitor_concurrent_pdv");

    var query = QueryBuilder
            .query()
            .table(our)
            .wildcardCoordinate(Datastore.SCENARIO_FIELD_NAME)
            .wildcardCoordinate("ean")
            .aggregatedMeasure("capdv", "sum")
            .expressionMeasure("capdv_concurrents", "sum(competitor_price * quantity)")
            .expressionMeasure("indice_prix", "sum(capdv) / sum(competitor_price * quantity)");

    this.mvc.perform(MockMvcRequestBuilders.post(SparkQueryController.MAPPING_QUERY)
                    .content(JacksonUtil.serialize(query))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              Map queryResult = JacksonUtil.mapper.readValue(contentAsString, Map.class);
              Assertions.assertThat((List) queryResult.get("rows")).containsExactlyInAnyOrder(
                      List.of("MN & MDD up", "Nutella 250g", 110000d, 102000d, 1.0784313725490196),
                      List.of("MN & MDD up", "ITMella 250g", 110000d, 102000d, 1.0784313725490196),

                      List.of("MN up", "Nutella 250g", 110000d, 102000d, 1.0784313725490196),
                      List.of("MN up", "ITMella 250g", 100000d, 102000d, 0.9803921568627451d),

                      List.of("MDD up", "ITMella 250g", 110000d, 102000d, 1.0784313725490196d),
                      List.of("MDD up", "Nutella 250g", 100000d, 102000d, 0.9803921568627451d),

                      List.of("MN & MDD down", "Nutella 250g", 90000d, 102000d, 0.8823529411764706),
                      List.of("MN & MDD down", "ITMella 250g", 90000d, 102000d, 0.8823529411764706),

                      List.of(MAIN_SCENARIO_NAME, "ITMella 250g", 100000d, 102000d, 0.9803921568627451d),
                      List.of(MAIN_SCENARIO_NAME, "Nutella 250g", 100000d, 102000d, 0.9803921568627451d));

              Assertions.assertThat((List) queryResult.get("columns")).containsExactly(
                      SCENARIO_FIELD_NAME, "ean", "sum(capdv)", "capdv_concurrents", "indice_prix");
            });
  }

  @Test
  public void testQueryWithRepo() throws Exception {
    var our = QueryBuilder.table("our_prices");
    var their = QueryBuilder.table("their_prices");
    var our_to_their = QueryBuilder.table("our_stores_their_stores");
    our.innerJoin(our_to_their, "pdv", "our_store");
    our_to_their.innerJoin(their, "their_store", "competitor_concurrent_pdv");

    var query = QueryBuilder
            .query()
            .table(our)
            .wildcardCoordinate(Datastore.SCENARIO_FIELD_NAME)
            .wildcardCoordinate("ean")
            .aggregatedMeasure("capdv", "sum")
            .unresolvedExpressionMeasure("capdv_concurrents")
            .unresolvedExpressionMeasure("indice_prix")
            .context(Repository.KEY, new Repository(REPO_URL));

    this.mvc.perform(MockMvcRequestBuilders.post(SparkQueryController.MAPPING_QUERY)
                    .content(JacksonUtil.serialize(query))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              Map queryResult = JacksonUtil.mapper.readValue(contentAsString, Map.class);
              Assertions.assertThat((List) queryResult.get("rows")).containsExactlyInAnyOrder(
                      List.of("MN & MDD up", "Nutella 250g", 110000d, 102000d, 1.0784313725490196),
                      List.of("MN & MDD up", "ITMella 250g", 110000d, 102000d, 1.0784313725490196),

                      List.of("MN up", "Nutella 250g", 110000d, 102000d, 1.0784313725490196),
                      List.of("MN up", "ITMella 250g", 100000d, 102000d, 0.9803921568627451d),

                      List.of("MDD up", "ITMella 250g", 110000d, 102000d, 1.0784313725490196d),
                      List.of("MDD up", "Nutella 250g", 100000d, 102000d, 0.9803921568627451d),

                      List.of("MN & MDD down", "Nutella 250g", 90000d, 102000d, 0.8823529411764706),
                      List.of("MN & MDD down", "ITMella 250g", 90000d, 102000d, 0.8823529411764706),

                      List.of(MAIN_SCENARIO_NAME, "ITMella 250g", 100000d, 102000d, 0.9803921568627451d),
                      List.of(MAIN_SCENARIO_NAME, "Nutella 250g", 100000d, 102000d, 0.9803921568627451d));

              Assertions.assertThat((List) queryResult.get("columns")).containsExactly(
                      SCENARIO_FIELD_NAME, "ean", "sum(capdv)", "capdv_concurrents", "indice_prix");
            });
  }

  @Test
  public void testQueryWithTotals() throws Exception {
    QueryDto query = new QueryDto()
            .table("our_prices")
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .context(Totals.KEY, QueryBuilder.TOP)
            .aggregatedMeasure("quantity", "sum");
    this.mvc.perform(MockMvcRequestBuilders.post(SparkQueryController.MAPPING_QUERY)
                    .content(JacksonUtil.serialize(query))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              SimpleTable table = JacksonUtil.mapper.readValue(contentAsString, SimpleTable.class);
              assertQueryWithTotals(table);
            });
  }

  public static void assertQueryWithTotals(SimpleTable table) {
    Assertions.assertThat(table.rows).containsExactlyInAnyOrder(
            Arrays.asList(QueryEngine.GRAND_TOTAL, 5 * 4000),
            List.of("MDD up", 4000),
            List.of("MN & MDD down", 4000),
            List.of("MN & MDD up", 4000),
            List.of("MN up", 4000),
            List.of(MAIN_SCENARIO_NAME, 4000)
    );
    Assertions.assertThat(table.columns).containsExactly(SCENARIO_FIELD_NAME, "sum(quantity)");
  }


  @Test
  void testMetadata() throws Exception {
    this.mvc.perform(MockMvcRequestBuilders.get(SparkQueryController.MAPPING_METADATA))
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              Map objects = JacksonUtil.mapper.readValue(contentAsString, Map.class);
              assertMetadataResult(objects, false);
            });
  }

  @Test
  void testMetadataWithRepository() throws Exception {
    this.mvc.perform(MockMvcRequestBuilders.get(SparkQueryController.MAPPING_METADATA)
                    .param("repo-url", REPO_URL))
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              Map objects = JacksonUtil.mapper.readValue(contentAsString, Map.class);
              assertMetadataResult(objects, true);
            });
  }


  static void assertMetadataResult(Map objects, boolean withRepo) {
    List<Map<String, Object>> storesArray = (List) objects.get(SparkQueryController.METADATA_STORES_KEY);
    Assertions.assertThat(storesArray).hasSize(3);

    Function<String, List<Map<Object, Object>>> f =
            storeName -> (List<Map<Object, Object>>) storesArray.stream().filter(s -> s.get("name").equals(storeName)).findFirst().get().get(SparkQueryController.METADATA_FIELDS_KEY);

    Assertions.assertThat(f.apply("our_prices")).containsExactlyInAnyOrder(
            Map.of("name", "ean", "type", "string"),
            Map.of("name", "pdv", "type", "string"),
            Map.of("name", "price", "type", "double"),
            Map.of("name", "quantity", "type", "int"),
            Map.of("name", "capdv", "type", "double"),
            Map.of("name", SCENARIO_FIELD_NAME, "type", "string")
    );

    Assertions.assertThat(f.apply("their_prices")).containsExactlyInAnyOrder(
            Map.of("name", "competitor_ean", "type", "string"),
            Map.of("name", "competitor_concurrent_pdv", "type", "string"),
            Map.of("name", "competitor_brand", "type", "string"),
            Map.of("name", "competitor_concurrent_ean", "type", "string"),
            Map.of("name", "competitor_price", "type", "double"),
            Map.of("name", SCENARIO_FIELD_NAME, "type", "string")
    );

    Assertions.assertThat(f.apply("our_stores_their_stores")).containsExactlyInAnyOrder(
            Map.of("name", "our_store", "type", "string"),
            Map.of("name", "their_store", "type", "string"),
            Map.of("name", SCENARIO_FIELD_NAME, "type", "string")
    );

    Assertions.assertThat((List) objects.get(SparkQueryController.METADATA_AGG_FUNCS_KEY)).containsExactlyInAnyOrder(SparkQueryController.SUPPORTED_AGG_FUNCS.toArray(new String[0]));
    if (withRepo) {
      Assertions.assertThat((List) objects.get(SparkQueryController.METADATA_METRICS_KEY)).containsExactlyInAnyOrder(
              Map.of("alias", "indice-prix", "expression", "sum(`numerateur-indice`) / sum(`score-visi`)"),
              Map.of("alias", "marge", "expression", "sum(`marge`)")
      );
    }
  }

  @Test
  public void testScenarioGroupingQuery() throws Exception {
    Map<String, List<String>> groups = new LinkedHashMap<>();
    groups.put("group1", List.of("base", "mdd-baisse-simu-sensi"));
    groups.put("group2", List.of("base", "mdd-baisse"));
    groups.put("group3", List.of("base", "mdd-baisse-simu-sensi", "mdd-baisse"));

    AggregatedMeasure aggregatedMeasure = new AggregatedMeasure("marge", "sum");
    ExpressionMeasure expressionMeasure = new ExpressionMeasure("indice-prix", "100 * sum(`numerateur-indice`) / sum" +
            "(`score-visi`)");
    ScenarioGroupingQueryDto query = new ScenarioGroupingQueryDto()
            .table("products")
            .groups(groups)
            .addScenarioComparison(new ScenarioComparisonDto(COMPARISON_METHOD_ABS_DIFF, aggregatedMeasure, false,
                    REF_POS_PREVIOUS))
            .addScenarioComparison(new ScenarioComparisonDto(COMPARISON_METHOD_ABS_DIFF, expressionMeasure, false,
                    REF_POS_PREVIOUS));

    this.mvc.perform(MockMvcRequestBuilders.post(SparkQueryController.MAPPING_QUERY_GROUPING)
                    .content(JacksonUtil.serialize(query))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              Map queryResult = JacksonUtil.mapper.readValue(contentAsString, Map.class);
              Assertions.assertThat((List) queryResult.get("rows")).containsExactly(
                      List.of("group1", "base", 0d, 0d),
                      List.of("group1", "mdd-baisse-simu-sensi", -90.00000000000003, -7.500000000000014),
                      List.of("group2", "base", 0d, 0d),
                      List.of("group2", "mdd-baisse", -40.00000000000003, -3.333333333333343),
                      List.of("group3", "base", 0d, 0d),
                      List.of("group3", "mdd-baisse-simu-sensi", -90.00000000000003, -7.500000000000014),
                      List.of("group3", "mdd-baisse", 50.0, 4.166666666666671));
              Assertions.assertThat((List) queryResult.get("columns")).containsExactly(
                      "group", SCENARIO_FIELD_NAME,
                      "absolute_difference(sum(marge), previous)", "absolute_difference(indice-prix, previous)");
            });
  }

  @Test
  public void testScenarioGroupingQueryWithRepo() throws Exception {
    Map<String, List<String>> groups = new LinkedHashMap<>();
    groups.put("group1", List.of("base", "mdd-baisse-simu-sensi"));
    groups.put("group2", List.of("base", "mdd-baisse"));
    groups.put("group3", List.of("base", "mdd-baisse-simu-sensi", "mdd-baisse"));

    AggregatedMeasure aggregatedMeasure = new AggregatedMeasure("marge", "sum");
    Measure unresolvedExpressionMeasure = new UnresolvedExpressionMeasure("indice-prix");
    ScenarioGroupingQueryDto query = new ScenarioGroupingQueryDto()
            .table("products")
            .groups(groups)
            .addScenarioComparison(new ScenarioComparisonDto(COMPARISON_METHOD_ABS_DIFF, aggregatedMeasure, false,
                    REF_POS_PREVIOUS))
            .addScenarioComparison(new ScenarioComparisonDto(COMPARISON_METHOD_ABS_DIFF, unresolvedExpressionMeasure,
                    false, REF_POS_PREVIOUS))
            .context(Repository.KEY, new Repository(REPO_URL));

    this.mvc.perform(MockMvcRequestBuilders.post(SparkQueryController.MAPPING_QUERY_GROUPING)
                    .content(JacksonUtil.serialize(query))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              Map queryResult = JacksonUtil.mapper.readValue(contentAsString, Map.class);
              Assertions.assertThat((List) queryResult.get("rows")).containsExactly(
                      List.of("group1", "base", 0d, 0d),
                      List.of("group1", "mdd-baisse-simu-sensi", -90.00000000000003, -0.07500000000000018),
                      List.of("group2", "base", 0d, 0d),
                      List.of("group2", "mdd-baisse", -40.00000000000003, -0.03333333333333344),
                      List.of("group3", "base", 0d, 0d),
                      List.of("group3", "mdd-baisse-simu-sensi", -90.00000000000003, -0.07500000000000018),
                      List.of("group3", "mdd-baisse", 50.0, 0.04166666666666674));
              Assertions.assertThat((List) queryResult.get("columns")).containsExactly(
                      "group", SCENARIO_FIELD_NAME,
                      "absolute_difference(sum(marge), previous)", "absolute_difference(indice-prix, previous)");
            });
  }
}