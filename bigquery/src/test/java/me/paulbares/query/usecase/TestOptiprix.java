package me.paulbares.query.usecase;

import me.paulbares.BigQueryDatastore;
import me.paulbares.BigQueryUtil;
import me.paulbares.query.*;
import me.paulbares.query.database.BigQueryEngine;
import me.paulbares.query.dto.CacheStatsDto;
import me.paulbares.query.dto.JoinMappingDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.TableDto;
import me.paulbares.query.monitoring.QueryWatch;
import me.paulbares.store.Datastore;
import me.paulbares.util.SchemaTypeScriptCodeGenerator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static me.paulbares.query.Functions.*;
import static me.paulbares.query.dto.JoinType.INNER;
import static me.paulbares.query.dto.JoinType.LEFT;

public class TestOptiprix {

  String credendialsPath = "/Users/paul/dev/aitmindiceprix-686299293f2f.json"; // FIXME
  String projectId = "aitmindiceprix";
  String datasetName = "optiprix";

  @Test
  @Disabled
  void test() {
    BigQueryDatastore datastore = new BigQueryDatastore(BigQueryUtil.createCredentials(this.credendialsPath), this.projectId, this.datasetName);
    BigQueryEngine engine = new BigQueryEngine(datastore);
    QueryExecutor executor = new QueryExecutor(engine);

    QueryDto subQuery = subQuery();

    QueryDto query = new QueryDto()
            .table(subQuery)
            .withMeasure(new ExpressionMeasure("InitialPriceIndex", "100*sum(itmInitialComparableTurnover)/sum(competitorComparableTurnover)"))
            .withMeasure(new ExpressionMeasure("RecommendedPriceIndex", "100*sum(itmRecommendedComparableTurnover)/sum(competitorComparableTurnover)"))
            .withMeasure(new ExpressionMeasure("FinalPriceIndex", "100*sum(itmFinalComparableTurnover)/sum(competitorComparableTurnover)"));

    QueryWatch queryWatch = new QueryWatch();
    CacheStatsDto.CacheStatsDtoBuilder csBuilder = CacheStatsDto.builder();
    Table execute = executor.execute(query, queryWatch, csBuilder);
    execute.show();
    System.out.println(queryWatch);
    System.out.println(csBuilder);
  }

  private static QueryDto subQuery() {
    TableDto recommendation = new TableDto("recommendation_with_simulation");
    TableDto current_selling_prices = new TableDto("current_selling_prices");
    TableDto competitor_catchment_area = new TableDto("competitor_catchment_area");
    TableDto competitor_price = new TableDto("competitor_price");
    TableDto competitor_store = new TableDto("competitor_store");

    recommendation.join(current_selling_prices, INNER,
            List.of(new JoinMappingDto(recommendation.name, "rec_ean", current_selling_prices.name, "cur_ean"),
                    new JoinMappingDto(recommendation.name, "rec_store_id", current_selling_prices.name, "cur_store_id")));
    recommendation.innerJoin(competitor_catchment_area, "rec_store_id", "cca_store_id");

    recommendation.join(competitor_price, INNER, List.of(
            new JoinMappingDto(competitor_catchment_area.name, "cca_competitor_store_id", competitor_price.name, "cp_store_id"),
            new JoinMappingDto(recommendation.name, "rec_ean", competitor_price.name, "cp_ean")));
    recommendation.join(competitor_store, LEFT,
            List.of(new JoinMappingDto(competitor_price.name, "cp_store_id", competitor_store.name, "cs_store_id"),
                    new JoinMappingDto(competitor_price.name, "cp_store_type", competitor_store.name, "cs_store_type")));

    Measure itmInitialComparableTurnover = multiply("itmInitialComparableTurnover",
            min("min_rec_initial_price", "rec_initial_price"),
            min("min_cur_vmm", "cur_vmm"));
    Measure itmRecommendedComparableTurnover = multiply("itmRecommendedComparableTurnover",
            min("min_rec_recommended_price", "rec_recommended_price"),
            min("min_cur_vmm", "cur_vmm"));
    Measure itmFinalComparableTurnover = multiply("itmFinalComparableTurnover",
            min("min_rec_final_price", "rec_final_price"),
            min("min_cur_vmm", "cur_vmm"));
    Measure competitorComparableTurnover = multiply("competitorComparableTurnover",
            avg("avg_cp_gross_price", "cp_gross_price"),
            min("min_cur_vmm", "cur_vmm"));

    QueryDto query = new QueryDto()
            .table(recommendation)
            .withMeasure(CountMeasure.INSTANCE)
            .withMeasure(itmInitialComparableTurnover)
            .withMeasure(itmRecommendedComparableTurnover)
            .withMeasure(itmFinalComparableTurnover)
            .withMeasure(competitorComparableTurnover);

    query.withCondition("rec_ean", eq(3346029200241L));
    query.withCondition("rec_store_id", in(1037, 1088, 1117, 1147, 1149));

    query
            .withColumn("rec_ean")
            .withColumn("rec_store_id")
            .withColumn("cs_company")
    ;
    return query;
  }

  @Test
  @Disabled
  void test2() {
    var initialPrice = avg("Prix_initial(AVG)", "rec_initial_price");
    var recommendedPrice = avg(
            "Prix_recommande(AVG)",
            "rec_recommended_price"
    );
    var finalPrice = avg("Prix_final(AVG)", "rec_final_price");

    var priceVariation = minus("Variation_de_prix_EUR", finalPrice, initialPrice);

    var pricingKeyLevelSubQuery = new QueryDto()
            .table(new TableDto("recommendation_with_simulation"))
            .withMeasure(priceVariation)
            .withColumn("rec_ean")
            .withColumn("rec_store_id")
            .withColumn("rec_simulation");

    var decreasePricesAvg = new AggregatedMeasure(
            "Baisse_moyenne_des_prix_en_baisse_EUR",
            "Variation_de_prix_EUR",
            "avg",
            criterion("Variation_de_prix_EUR", lt(0))
    );

    BigQueryDatastore datastore = new BigQueryDatastore(BigQueryUtil.createCredentials(this.credendialsPath), this.projectId, this.datasetName);
    BigQueryEngine engine = new BigQueryEngine(datastore);
    QueryExecutor executor = new QueryExecutor(engine);

    QueryDto query = new QueryDto()
            .table(pricingKeyLevelSubQuery)
            .withColumn("rec_simulation")
            .withMeasure(decreasePricesAvg);
    QueryWatch queryWatch = new QueryWatch();
    CacheStatsDto.CacheStatsDtoBuilder csBuilder = CacheStatsDto.builder();
    Table execute = executor.execute(query, queryWatch, csBuilder);
    execute.show();
    System.out.println(queryWatch);
    System.out.println(csBuilder);
  }

  @Test
  @Disabled
  void testTypescriptGeneration() {
    Datastore datastore = new BigQueryDatastore(BigQueryUtil.createCredentials(this.credendialsPath), this.projectId, this.datasetName);
    System.out.println(SchemaTypeScriptCodeGenerator.getFileContent(datastore));
  }
}