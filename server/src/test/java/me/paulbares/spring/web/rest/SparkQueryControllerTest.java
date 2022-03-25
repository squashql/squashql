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

import static me.paulbares.query.ScenarioGroupingExecutor.COMPARISON_METHOD_ABS_DIFF;
import static me.paulbares.query.ScenarioGroupingExecutor.REF_POS_PREVIOUS;
import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;
import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
public class SparkQueryControllerTest {

  private static final String REPO_URL = "https://raw.githubusercontent.com/paulbares/aitm-assets/main/metrics.json";

  @Autowired
  private MockMvc mvc;

  @Test
  public void testQuery() throws Exception {
    QueryDto query = new QueryDto()
            .table("products")
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .aggregatedMeasure("marge", "sum")
            .expressionMeasure("indice-prix", "100 * sum(`numerateur-indice`) / sum(`score-visi`)");
    this.mvc.perform(MockMvcRequestBuilders.post(SparkQueryController.MAPPING_QUERY)
                    .content(JacksonUtil.serialize(query))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              Map queryResult = JacksonUtil.mapper.readValue(contentAsString, Map.class);
              Assertions.assertThat((List) queryResult.get("rows")).containsExactlyInAnyOrder(
                      List.of(MAIN_SCENARIO_NAME, 280.00000000000006d, 110.44985250737464),
                      List.of("mdd-baisse-simu-sensi", 190.00000000000003d, 102.94985250737463d),
                      List.of("mdd-baisse", 240.00000000000003d, 107.1165191740413d)
              );
              Assertions.assertThat((List) queryResult.get("columns")).containsExactly(SCENARIO_FIELD_NAME, "sum(marge)", "indice-prix");
            });
  }

  @Test
  public void testQueryWithRepo() throws Exception {
    QueryDto query = new QueryDto()
            .table("products")
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .unresolvedExpressionMeasure("marge")
            .unresolvedExpressionMeasure("indice-prix")
            .context(Repository.KEY, new Repository(REPO_URL));
    this.mvc.perform(MockMvcRequestBuilders.post(SparkQueryController.MAPPING_QUERY)
                    .content(JacksonUtil.serialize(query))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              Map queryResult = JacksonUtil.mapper.readValue(contentAsString, Map.class);
              Assertions.assertThat((List) queryResult.get("rows")).containsExactlyInAnyOrder(
                      List.of(MAIN_SCENARIO_NAME, 280.00000000000006d, 110.44985250737464 / 100),
                      List.of("mdd-baisse-simu-sensi", 190.00000000000003d, 102.94985250737463d / 100),
                      List.of("mdd-baisse", 240.00000000000003d, 107.1165191740413d / 100)
              );
              Assertions.assertThat((List) queryResult.get("columns")).containsExactly(SCENARIO_FIELD_NAME, "marge", "indice-prix");
            });
  }

  @Test
  public void testQueryWithTotals() throws Exception {
    QueryDto query = new QueryDto()
            .table("products")
            .wildcardCoordinate(SCENARIO_FIELD_NAME)
            .context(Totals.KEY, QueryBuilder.TOP)
            .aggregatedMeasure("marge", "sum");
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

  static void assertQueryWithTotals(SimpleTable table) {
    Assertions.assertThat(table.rows).containsExactlyInAnyOrder(
            Arrays.asList(QueryEngine.GRAND_TOTAL, 280.00000000000006d + 190.00000000000003d + 240.00000000000003d),
            List.of(MAIN_SCENARIO_NAME, 280.00000000000006d),
            List.of("mdd-baisse-simu-sensi", 190.00000000000003d),
            List.of("mdd-baisse", 240.00000000000003d)
    );
    Assertions.assertThat(table.columns).containsExactly(SCENARIO_FIELD_NAME, "sum(marge)");
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
    Assertions.assertThat(storesArray).hasSize(1);
    Assertions.assertThat(storesArray.get(0).get("name")).isEqualTo("products");
    Assertions.assertThat((List) storesArray.get(0).get(SparkQueryController.METADATA_FIELDS_KEY)).containsExactlyInAnyOrder(
            Map.of("name", "ean", "type", "string"),
            Map.of("name", "pdv", "type", "string"),
            Map.of("name", "categorie", "type", "string"),
            Map.of("name", "type-marque", "type", "string"),
            Map.of("name", "sensibilite", "type", "string"),
            Map.of("name", "quantite", "type", "int"),
            Map.of("name", "prix", "type", "double"),
            Map.of("name", "achat", "type", "int"),
            Map.of("name", "score-visi", "type", "int"),
            Map.of("name", "min-marche", "type", "double"),
            Map.of("name", "ca", "type", "double"),
            Map.of("name", "marge", "type", "double"),
            Map.of("name", "numerateur-indice", "type", "double"),
            Map.of("name", "indice-prix", "type", "double"),
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
    ExpressionMeasure expressionMeasure = new ExpressionMeasure("indice-prix", "100 * sum(`numerateur-indice`) / sum(`score-visi`)");
    ScenarioGroupingQueryDto query = new ScenarioGroupingQueryDto()
            .table("products")
            .groups(groups)
            .addScenarioComparison(new ScenarioComparisonDto(COMPARISON_METHOD_ABS_DIFF, aggregatedMeasure, false, REF_POS_PREVIOUS))
            .addScenarioComparison(new ScenarioComparisonDto(COMPARISON_METHOD_ABS_DIFF, expressionMeasure, false, REF_POS_PREVIOUS));

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
            .addScenarioComparison(new ScenarioComparisonDto(COMPARISON_METHOD_ABS_DIFF, aggregatedMeasure, false, REF_POS_PREVIOUS))
            .addScenarioComparison(new ScenarioComparisonDto(COMPARISON_METHOD_ABS_DIFF, unresolvedExpressionMeasure, false, REF_POS_PREVIOUS))
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