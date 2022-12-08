package me.paulbares.query;

import java.sql.SQLException;
import me.paulbares.SnowflakeDatastore;
import me.paulbares.SnowflakeUtil;
import me.paulbares.query.database.SnowflakeEngine;
import me.paulbares.query.dto.CacheStatsDto;
import me.paulbares.query.dto.JoinMappingDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.TableDto;
import me.paulbares.query.monitoring.QueryWatch;
import me.paulbares.store.Datastore;
import me.paulbares.util.TableTSCodeGenerator;
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
  void test() throws SQLException {
    SnowflakeDatastore datastore = new SnowflakeDatastore();
//    SnowflakeDatastore datastore = new SnowflakeDatastore(SnowflakeUtil.createCredentials(this.credendialsPath), this.projectId, this.datasetName);
    SnowflakeEngine engine = new SnowflakeEngine(datastore);
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
    TableDto recommendation = new TableDto("RECOMMENDATION_WITH_SIMULATION");
    TableDto current_selling_prices = new TableDto("CURRENT_SELLING_PRICES");
    TableDto competitor_catchment_area = new TableDto("COMPETITOR_CATCHMENT_AREA");
    TableDto competitor_price = new TableDto("COMPETITOR_PRICE_WITH_MATCHING");
    TableDto competitor_store = new TableDto("COMPETITOR_STORE");

    recommendation.join(current_selling_prices, INNER,
            List.of(new JoinMappingDto(recommendation.name, "REC_EAN", current_selling_prices.name, "CUR_EAN"),
                    new JoinMappingDto(recommendation.name, "REC_STORE_ID", current_selling_prices.name, "CUR_STORE_ID")));
    recommendation.innerJoin(competitor_catchment_area, "REC_STORE_ID", "CCA_STORE_ID");

    recommendation.join(competitor_price, INNER, List.of(
            new JoinMappingDto(competitor_catchment_area.name, "CCA_COMPETITOR_STORE_ID", competitor_price.name, "CP_STORE_ID"),
            new JoinMappingDto(recommendation.name, "REC_EAN", competitor_price.name, "CP_EAN")));
    recommendation.join(competitor_store, LEFT,
            List.of(new JoinMappingDto(competitor_price.name, "CP_STORE_ID", competitor_store.name, "CS_STORE_ID"),
                    new JoinMappingDto(competitor_price.name, "CP_STORE_TYPE", competitor_store.name, "CS_STORE_TYPE")));

    Measure itmInitialComparableTurnover = multiply("itmInitialComparableTurnover",
            min("min_rec_initial_price", "REC_INITIAL_PRICE"),
            min("min_cur_vmm", "CUR_VMM"));
    Measure itmRecommendedComparableTurnover = multiply("itmRecommendedComparableTurnover",
            min("min_rec_recommended_price", "REC_RECOMMENDED_PRICE"),
            min("min_cur_vmm", "CUR_VMM"));
    Measure itmFinalComparableTurnover = multiply("itmFinalComparableTurnover",
            min("min_rec_final_price", "REC_FINAL_PRICE"),
            min("min_cur_vmm", "CUR_VMM"));
    Measure competitorComparableTurnover = multiply("competitorComparableTurnover",
            avg("avg_cp_gross_price", "CP_GROSS_PRICE"),
            min("min_cur_vmm", "CUR_VMM"));

    QueryDto query = new QueryDto()
            .table(recommendation)
            .withMeasure(CountMeasure.INSTANCE)
            .withMeasure(itmInitialComparableTurnover)
            .withMeasure(itmRecommendedComparableTurnover)
            .withMeasure(itmFinalComparableTurnover)
            .withMeasure(competitorComparableTurnover);

    query.withCondition("REC_EAN", eq(3346029200241L));
    query.withCondition("REC_STORE_ID", in(1037, 1088, 1117, 1147, 1149));

    query
            .withColumn("REC_EAN")
            .withColumn("REC_STORE_ID")
            .withColumn("CS_COMPANY")
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
            "Variation_de_prix_EUR",
            lt(0)
    );

//    SnowflakeDatastore datastore = new SnowflakeDatastore(SnowflakeUtil.createCredentials(this.credendialsPath), this.projectId, this.datasetName);
//    SnowflakeEngine engine = new SnowflakeEngine(datastore);
//    QueryExecutor executor = new QueryExecutor(engine);
//
//    QueryDto query = new QueryDto()
//            .table(pricingKeyLevelSubQuery)
//            .withColumn("rec_simulation")
//            .withMeasure(decreasePricesAvg);
//    QueryWatch queryWatch = new QueryWatch();
//    CacheStatsDto.CacheStatsDtoBuilder csBuilder = CacheStatsDto.builder();
//    Table execute = executor.execute(query, queryWatch, csBuilder);
//    execute.show();
//    System.out.println(queryWatch);
//    System.out.println(csBuilder);
  }

  @Test
  @Disabled
  void testTSGeneration() {
//    Datastore datastore = new SnowflakeDatastore(SnowflakeUtil.createCredentials(this.credendialsPath), this.projectId, this.datasetName);
//    System.out.println(TableTSCodeGenerator.getFileContent(datastore));
  }
}
