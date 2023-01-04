package io.squashql.spring.web.rest;

import io.squashql.jackson.JacksonUtil;
import io.squashql.query.*;
import io.squashql.query.builder.Query;
import io.squashql.query.database.SparkQueryEngine;
import io.squashql.query.dto.*;
import io.squashql.spring.dataset.DatasetTestConfig;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.squashql.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static io.squashql.transaction.TransactionManager.SCENARIO_FIELD_NAME;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "spring.main.allow-bean-definition-overriding=true")
@Import(DatasetTestConfig.class)
@AutoConfigureMockMvc
public class QueryControllerTest {

  @Autowired
  private MockMvc mvc;

  @Autowired
  private Environment env;

  @Test
  void testQuery() throws Exception {
    var query = Query
            .from("our_prices")
            .innerJoin("our_stores_their_stores")
            .on("our_prices", "pdv", "our_stores_their_stores", "our_store")
            .innerJoin("their_prices")
            .on("their_prices", "competitor_concurrent_pdv", "our_stores_their_stores", "their_store")
            .select(List.of(SCENARIO_FIELD_NAME, "ean"), List.of(
                    Functions.sum("capdv", "capdv"),
                    new ExpressionMeasure("capdv_concurrents", "sum(competitor_price * quantity)"),
                    new ExpressionMeasure("indice_prix", "sum(capdv) / sum(competitor_price * quantity)")))
            .build();

    this.mvc.perform(MockMvcRequestBuilders.post(QueryController.MAPPING_QUERY)
                    .content(JacksonUtil.serialize(query))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              QueryResultDto queryResult = JacksonUtil.deserialize(contentAsString, QueryResultDto.class);
              Assertions.assertThat(queryResult.table.rows).containsExactlyInAnyOrder(
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

              Assertions.assertThat(queryResult.table.columns).containsExactly(
                      SCENARIO_FIELD_NAME, "ean", "capdv", "capdv_concurrents", "indice_prix");

              Assertions.assertThat(queryResult.metadata).containsExactly(
                      new MetadataItem(SCENARIO_FIELD_NAME, SCENARIO_FIELD_NAME, String.class),
                      new MetadataItem("ean", "ean", String.class),
                      new MetadataItem("capdv", "sum(capdv)", double.class),
                      new MetadataItem("capdv_concurrents", "sum(competitor_price * quantity)", double.class),
                      new MetadataItem("indice_prix", "sum(capdv) / sum(competitor_price * quantity)", double.class)
              );

              Assertions.assertThat(queryResult.debug.cache).isNotNull();
              Assertions.assertThat(queryResult.debug.timings.total).isGreaterThan(0);
            });
  }

  @Test
  void testMetadata() throws Exception {
    this.mvc.perform(MockMvcRequestBuilders.get(QueryController.MAPPING_METADATA))
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              assertMetadataResult(JacksonUtil.mapper.readValue(contentAsString, MetadataResultDto.class));
            });
  }

  public static void assertMetadataResult(MetadataResultDto metadataResultDto) {
    Function<String, MetadataResultDto.StoreMetadata> f =
            storeName -> (MetadataResultDto.StoreMetadata) metadataResultDto.stores.stream().filter(s -> s.name.equals(storeName)).findFirst().get();

    Assertions.assertThat(f.apply("our_prices").fields).containsExactlyInAnyOrder(
            new MetadataItem("ean", "ean", String.class),
            new MetadataItem("pdv", "pdv", String.class),
            new MetadataItem("price", "price", double.class),
            new MetadataItem("quantity", "quantity", int.class),
            new MetadataItem("capdv", "capdv", double.class),
            new MetadataItem(SCENARIO_FIELD_NAME, SCENARIO_FIELD_NAME, String.class)
    );

    Assertions.assertThat(f.apply("their_prices").fields).containsExactlyInAnyOrder(
            new MetadataItem("competitor_ean", "competitor_ean", String.class),
            new MetadataItem("competitor_concurrent_pdv", "competitor_concurrent_pdv", String.class),
            new MetadataItem("competitor_brand", "competitor_brand", String.class),
            new MetadataItem("competitor_concurrent_ean", "competitor_concurrent_ean", String.class),
            new MetadataItem("competitor_price", "competitor_price", double.class)
    );

    Assertions.assertThat(f.apply("our_stores_their_stores").fields).containsExactlyInAnyOrder(
            new MetadataItem("our_store", "our_store", String.class),
            new MetadataItem("their_store", "their_store", String.class)
    );

    Assertions.assertThat(metadataResultDto.aggregationFunctions).containsExactlyInAnyOrder(SparkQueryEngine.SUPPORTED_AGGREGATION_FUNCTIONS.toArray(new String[0]));
  }

  @Test
  void testScenarioGroupingQuery() throws Exception {
    BucketColumnSetDto bucketCS = new BucketColumnSetDto("group", SCENARIO_FIELD_NAME)
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "MN up"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "MN & MDD up"))
            .withNewBucket("group3", List.of(MAIN_SCENARIO_NAME, "MN up", "MN & MDD up"));

    Measure aggregatedMeasure = Functions.sum("capdv", "capdv");
    Measure indicePrix = new ExpressionMeasure(
            "indice_prix",
            "sum(capdv) / sum(competitor_price * quantity)");
    ComparisonMeasureReferencePosition aggregatedMeasureDiff = new ComparisonMeasureReferencePosition(
            "aggregatedMeasureDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            aggregatedMeasure,
            Map.of(SCENARIO_FIELD_NAME, "s-1", "group", "g"),
            ColumnSetKey.BUCKET);
    ComparisonMeasureReferencePosition indicePrixDiff = new ComparisonMeasureReferencePosition(
            "indicePrixDiff",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            indicePrix,
            Map.of(SCENARIO_FIELD_NAME, "s-1", "group", "g"),
            ColumnSetKey.BUCKET);

    var query = Query
            .from("our_prices")
            .innerJoin("our_stores_their_stores")
            .on("our_prices", "pdv", "our_stores_their_stores", "our_store")
            .innerJoin("their_prices")
            .on("their_prices", "competitor_concurrent_pdv", "our_stores_their_stores", "their_store")
            .select_(List.of(bucketCS), List.of(aggregatedMeasureDiff, indicePrixDiff))
            .build();

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
