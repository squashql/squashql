package me.paulbares.spring.web.rest;

import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.*;
import me.paulbares.query.context.Repository;
import me.paulbares.query.dto.BucketColumnSetDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.TableDto;
import me.paulbares.query.monitoring.QueryWatch;
import me.paulbares.spring.dataset.DatasetTestConfig;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static me.paulbares.query.TableUtils.*;
import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static me.paulbares.transaction.TransactionManager.SCENARIO_FIELD_NAME;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "spring.main.allow-bean-definition-overriding=true")
@Import(DatasetTestConfig.class)
@AutoConfigureMockMvc
public class QueryControllerTest {

  private static final String REPO_URL = "https://raw.githubusercontent" +
          ".com/paulbares/aitm-assets/main/metrics-controller-test.json";

  @Autowired
  private MockMvc mvc;

  private TableDto createTableDto() {
    var our = QueryBuilder.table("our_prices");
    var their = QueryBuilder.table("their_prices");
    var our_to_their = QueryBuilder.table("our_stores_their_stores");
    our.innerJoin(our_to_their, "pdv", "our_store");
    our_to_their.innerJoin(their, "their_store", "competitor_concurrent_pdv");
    return our;
  }

  @Test
  void testQueryWithoutRepo() throws Exception {
    testQuery(false);
  }

  @Test
  void testQueryWithRepo() throws Exception {
    testQuery(true);
  }

  void testQuery(boolean withRepo) throws Exception {
    var our = createTableDto();

    var query = new QueryDto()
            .table(our)
            .withColumn(SCENARIO_FIELD_NAME)
            .withColumn("ean")
            .aggregatedMeasure("capdv", "sum");

    if (withRepo) {
      query
              .unresolvedExpressionMeasure("capdv_concurrents")
              .unresolvedExpressionMeasure("indice_prix")
              .context(Repository.KEY, new Repository(REPO_URL));
    } else {
      query
              .expressionMeasure("capdv_concurrents", "sum(competitor_price * quantity)")
              .expressionMeasure("indice_prix", "sum(capdv) / sum(competitor_price * quantity)");
    }

    this.mvc.perform(MockMvcRequestBuilders.post(QueryController.MAPPING_QUERY)
                    .content(JacksonUtil.serialize(query))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              Map queryResult = JacksonUtil.mapper.readValue(contentAsString, Map.class);
              Map<String, Object> table = (Map<String, Object>) queryResult.get("table");
              Assertions.assertThat((List) table.get("rows")).containsExactlyInAnyOrder(
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

              Assertions.assertThat((List) table.get("columns")).containsExactly(
                      SCENARIO_FIELD_NAME, "ean", "sum(capdv)", "capdv_concurrents", "indice_prix");

              Assertions.assertThat((List) queryResult.get("metadata")).containsExactly(
                      Map.of(NAME_KEY, SCENARIO_FIELD_NAME, TYPE_KEY, "string"),
                      Map.of(NAME_KEY, "ean", TYPE_KEY, "string"),
                      Map.of(NAME_KEY, "sum(capdv)", TYPE_KEY, "double", EXPRESSION_KEY, "sum(capdv)"),
                      Map.of(NAME_KEY, "capdv_concurrents", TYPE_KEY, "double", EXPRESSION_KEY, "sum(competitor_price * quantity)"),
                      Map.of(NAME_KEY, "indice_prix", TYPE_KEY, "double", EXPRESSION_KEY, "sum(capdv) / sum(competitor_price * quantity)")
              );

              Map<String, Object> debug = (Map<String, Object>) queryResult.get("debug");
              Assertions.assertThat(((Map) debug.get("cache")).size()).isEqualTo(3);
              Assertions.assertThat(((Map) debug.get("timings")).keySet()).containsExactlyInAnyOrder(
                      QueryWatch.EXECUTE_PLAN,
                      QueryWatch.PREPARE_PLAN,
                      QueryWatch.BUCKET,
                      QueryWatch.PREFETCH,
                      QueryWatch.ORDER,
                      "total"
              );
            });
  }

  @Test
  void testMetadata() throws Exception {
    this.mvc.perform(MockMvcRequestBuilders.get(QueryController.MAPPING_METADATA))
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              Map objects = JacksonUtil.mapper.readValue(contentAsString, Map.class);
              assertMetadataResult(objects, false);
            });
  }

  @Test
  void testMetadataWithRepository() throws Exception {
    this.mvc.perform(MockMvcRequestBuilders.get(QueryController.MAPPING_METADATA)
                    .param("repo-url", REPO_URL))
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              Map objects = JacksonUtil.mapper.readValue(contentAsString, Map.class);
              assertMetadataResult(objects, true);
            });
  }

  public static void assertMetadataResult(Map objects, boolean withRepo) {
    List<Map<String, Object>> storesArray = (List) objects.get(QueryController.METADATA_STORES_KEY);
    Assertions.assertThat(storesArray).hasSize(3);

    Function<String, List<Map<Object, Object>>> f =
            storeName -> (List<Map<Object, Object>>) storesArray.stream().filter(s -> s.get("name").equals(storeName)).findFirst().get().get(QueryController.METADATA_FIELDS_KEY);

    Assertions.assertThat(f.apply("our_prices")).containsExactlyInAnyOrder(
            Map.of(NAME_KEY, "ean", TYPE_KEY, "string"),
            Map.of(NAME_KEY, "pdv", TYPE_KEY, "string"),
            Map.of(NAME_KEY, "price", TYPE_KEY, "double"),
            Map.of(NAME_KEY, "quantity", TYPE_KEY, "int"),
            Map.of(NAME_KEY, "capdv", TYPE_KEY, "double"),
            Map.of(NAME_KEY, SCENARIO_FIELD_NAME, TYPE_KEY, "string")
    );

    Assertions.assertThat(f.apply("their_prices")).containsExactlyInAnyOrder(
            Map.of(NAME_KEY, "competitor_ean", TYPE_KEY, "string"),
            Map.of(NAME_KEY, "competitor_concurrent_pdv", TYPE_KEY, "string"),
            Map.of(NAME_KEY, "competitor_brand", TYPE_KEY, "string"),
            Map.of(NAME_KEY, "competitor_concurrent_ean", TYPE_KEY, "string"),
            Map.of(NAME_KEY, "competitor_price", TYPE_KEY, "double")
    );

    Assertions.assertThat(f.apply("our_stores_their_stores")).containsExactlyInAnyOrder(
            Map.of(NAME_KEY, "our_store", TYPE_KEY, "string"),
            Map.of(NAME_KEY, "their_store", TYPE_KEY, "string")
    );

    Assertions.assertThat((List) objects.get(QueryController.METADATA_AGG_FUNCS_KEY)).containsExactlyInAnyOrder(QueryController.SUPPORTED_AGG_FUNCS.toArray(new String[0]));
    if (withRepo) {
      Assertions.assertThat((List) objects.get(QueryController.METADATA_MEASURES_KEY)).containsExactlyInAnyOrder(
              Map.of("alias", "indice_prix", EXPRESSION_KEY, "sum(capdv) / sum(competitor_price * quantity)"),
              Map.of("alias", "capdv_concurrents", EXPRESSION_KEY, "sum(competitor_price * quantity)")
      );
    }
  }

  @Test
  void testScenarioGroupingQueryWithoutRepo() throws Exception {
    testScenarioGroupingQuery(false);
  }

  @Test
  void testScenarioGroupingQueryWithRepo() throws Exception {
    testScenarioGroupingQuery(true);
  }

  private void testScenarioGroupingQuery(boolean withRepo) throws Exception {
    BucketColumnSetDto bucketCS = new BucketColumnSetDto("group", SCENARIO_FIELD_NAME)
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "MN up"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "MN & MDD up"))
            .withNewBucket("group3", List.of(MAIN_SCENARIO_NAME, "MN up", "MN & MDD up"));

    AggregatedMeasure aggregatedMeasure = new AggregatedMeasure("capdv", "sum");
    Measure indicePrix;
    if (withRepo) {
      indicePrix = new UnresolvedExpressionMeasure("indice_prix");
    } else {
      indicePrix = new ExpressionMeasure(
              "indice_prix",
              "sum(capdv) / sum(competitor_price * quantity)");
    }
    ComparisonMeasure aggregatedMeasureDiff = QueryBuilder.bucketComparison(
            "aggregatedMeasureDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            aggregatedMeasure,
            Map.of(
                    SCENARIO_FIELD_NAME, "s-1",
                    "group", "g"
            ));
    ComparisonMeasure indicePrixDiff = QueryBuilder.bucketComparison(
            "indicePrixDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            indicePrix,
            Map.of(
                    SCENARIO_FIELD_NAME, "s-1",
                    "group", "g"
            ));

    var query = new QueryDto()
            .table(createTableDto())
            .withColumnSet(QueryDto.BUCKET, bucketCS)
            .withMeasure(aggregatedMeasureDiff)
            .withMeasure(indicePrixDiff);

    if (withRepo) {
      query.context(Repository.KEY, new Repository(REPO_URL));
    }

    this.mvc.perform(MockMvcRequestBuilders.post(QueryController.MAPPING_QUERY)
                    .content(JacksonUtil.serialize(query))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              Map queryResult = JacksonUtil.mapper.readValue(contentAsString, Map.class);
              Map<String, Object> table = (Map<String, Object>) queryResult.get("table");

              double baseValue = 0.9803921568627451d;
              double mnValue = 1.0294117647058822d;
              double mnmddValue = 1.0784313725490196d;
              Assertions.assertThat((List) table.get("rows")).containsExactlyInAnyOrder(
                      List.of("group1", MAIN_SCENARIO_NAME, 0d, 0d),
                      List.of("group1", "MN up", 10_000d, mnValue - baseValue),
                      List.of("group2", MAIN_SCENARIO_NAME, 0d, 0d),
                      List.of("group2", "MN & MDD up", 20_000d, mnmddValue - baseValue),
                      List.of("group3", MAIN_SCENARIO_NAME, 0d, 0d),
                      List.of("group3", "MN up", 10_000d, mnValue - baseValue),
                      List.of("group3", "MN & MDD up", 10_000d, mnmddValue - mnValue));
              Assertions.assertThat((List) table.get("columns"))
                      .containsExactly("group",
                              SCENARIO_FIELD_NAME,
                              "aggregatedMeasureDiff",
                              "indicePrixDiff");
            });
  }
}
