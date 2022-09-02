package me.paulbares.query;

import me.paulbares.BigQueryDatastore;
import me.paulbares.BigQueryUtil;
import me.paulbares.query.database.BigQueryEngine;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.TableDto;
import org.junit.jupiter.api.Test;

import static me.paulbares.query.QueryBuilder.eq;
import static me.paulbares.query.QueryBuilder.in;

public class TestOptiprix {

  String credendialsPath = "/Users/paul/dev/aitmindiceprix-686299293f2f.json"; // FIXME
  String projectId = "aitmindiceprix";
  String datasetName = "optiprix";

  @Test
//  @Disabled
  void test() {
    BigQueryDatastore datastore = new BigQueryDatastore(BigQueryUtil.createCredentials(this.credendialsPath), this.projectId, this.datasetName);
    BigQueryEngine engine = new BigQueryEngine(datastore);
    QueryExecutor executor = new QueryExecutor(engine);

    TableDto recommandation = new TableDto("recommandation");
    TableDto current_selling_prices = new TableDto("current_selling_prices");
    TableDto competitor_catchment_area = new TableDto("competitor_catchment_area");
    TableDto competitor_price = new TableDto("competitor_price");
    TableDto competitor_store = new TableDto("competitor_store");

//    recommandation.join(current_selling_prices, "inner", List.of(new JoinMappingDto("rec_ean", "cur_ean"), new JoinMappingDto("rec_store_id", "cur_store_id")));
    recommandation.innerJoin(competitor_catchment_area, "rec_store_id", "cca_store_id");

    // FIXME this joins should generate someting like this:
//    inner join `aitmindiceprix.optiprix.competitor_price` on `aitmindiceprix.optiprix.competitor_catchment_area`.cca_competitor_store_id = `aitmindiceprix.optiprix.competitor_price`.cp_store_id and `aitmindiceprix.optiprix.recommandation`.rec_ean = `aitmindiceprix.optiprix.competitor_price`.cp_ean
//    competitor_catchment_area.join(competitor_price, "inner", List.of(
//            new JoinMappingDto("cca_competitor_store_id", "cp_store_id"),
//            new JoinMappingDto("rec_ean", "cp_ean")));
//    competitor_price.join(competitor_store, "left", List.of(new JoinMappingDto("cp_store_id", "cs_store_id"), new JoinMappingDto("cp_store_type", "cs_store_type")));

    QueryDto query = QueryBuilder.query().table(recommandation);
//    Measure itmInitialComparableTurnover = multiply("itmInitialComparableTurnover",
//            min("min_rec_initial_price", "rec_initial_price"),
//            min("min_cur_vmm", "cur_vmm"));
//    Measure itmRecommendedComparableTurnover = multiply("itmRecommendedComparableTurnover",
//            min("min_rec_recommended_price", "rec_recommended_price"),
//            min("min_cur_vmm", "cur_vmm"));
//    Measure itmFinalComparableTurnover = multiply("itmFinalComparableTurnover",
//            min("min_rec_final_price", "rec_final_price"),
//            min("min_cur_vmm", "cur_vmm"));
//    Measure competitorComparableTurnover = multiply("competitorComparableTurnover",
//            avg("avg_cp_gross_price", "cp_gross_price"),
//            min("min_cur_vmm", "cur_vmm"));
    query
            .withMeasure(CountMeasure.INSTANCE)
//            .withMeasure(itmInitialComparableTurnover)
//            .withMeasure(itmRecommendedComparableTurnover)
//            .withMeasure(itmFinalComparableTurnover)
//            .withMeasure(competitorComparableTurnover)
    ;

    query.withCondition("rec_ean", eq(3346029200241L));
    query.withCondition("rec_store_id", in(1037, 1088, 1117, 1147, 1149));

//    query
//            .withColumn("rec_ean")
//            .withColumn("rec_store_id")
//            .withColumn("cs_company")
    ;

    Table execute = executor.execute(query);
    execute.show();
  }
}
