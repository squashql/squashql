import {
  JoinMapping,
  JoinType,
  Querier,
  Query,
  Table,
  ExpressionMeasure,
  multiply,
  min,
  avg,
  eq,
  _in
} from "aitm-js-query"

const querier = new Querier("http://localhost:8080");

const toString = (a: any): string => JSON.stringify(a, null, 1)

// querier.getMetadata(assets).then(r => {
//   console.log(`Store: ${toString(r.stores)}`);
//   console.log(`Measures: ${toString(r.measures)}`)
//   console.log(`Agg Func: ${r.aggregationFunctions}`)
// })

const subQuery = new Query()
const recommandation = new Table("recommandation");
const current_selling_prices = new Table("current_selling_prices");
const competitor_catchment_area = new Table("competitor_catchment_area");
const competitor_price = new Table("competitor_price");
const competitor_store = new Table("competitor_store");

recommandation.join(current_selling_prices, JoinType.INNER, [
  new JoinMapping(recommandation.name, "rec_ean", current_selling_prices.name, "cur_ean"),
  new JoinMapping(recommandation.name, "rec_store_id", current_selling_prices.name, "cur_store_id")
])
recommandation.innerJoin(competitor_catchment_area, "rec_store_id", "cca_store_id");
recommandation.join(competitor_price, JoinType.INNER, [
  new JoinMapping(competitor_catchment_area.name, "cca_competitor_store_id", competitor_price.name, "cp_store_id"),
  new JoinMapping(recommandation.name, "rec_ean", competitor_price.name, "cp_ean")
])
recommandation.join(competitor_store, JoinType.LEFT, [
  new JoinMapping(competitor_price.name, "cp_store_id", competitor_store.name, "cs_store_id"),
  new JoinMapping(competitor_price.name, "cp_store_type", competitor_store.name, "cs_store_type")
])

const itmInitialComparableTurnover = multiply("itmInitialComparableTurnover",
        min("min_rec_initial_price", "rec_initial_price"),
        min("min_cur_vmm", "cur_vmm"));
const itmRecommendedComparableTurnover = multiply("itmRecommendedComparableTurnover",
        min("min_rec_recommended_price", "rec_recommended_price"),
        min("min_cur_vmm", "cur_vmm"));
const itmFinalComparableTurnover = multiply("itmFinalComparableTurnover",
        min("min_rec_final_price", "rec_final_price"),
        min("min_cur_vmm", "cur_vmm"));
const competitorComparableTurnover = multiply("competitorComparableTurnover",
        avg("avg_cp_gross_price", "cp_gross_price"),
        min("min_cur_vmm", "cur_vmm"));

subQuery.onTable(recommandation)
        .withMeasure(itmInitialComparableTurnover)
        .withMeasure(itmRecommendedComparableTurnover)
        .withMeasure(itmFinalComparableTurnover)
        .withMeasure(competitorComparableTurnover)

subQuery.withCondition("rec_ean", eq(3346029200241))
subQuery.withCondition("rec_store_id", _in([1037, 1088, 1117, 1147, 1149]))

subQuery
        .withColumn("rec_ean")
        .withColumn("rec_store_id")
        .withColumn("cs_company")

const q = new Query()
        .onVirtualTable(subQuery)
        .withMeasure(new ExpressionMeasure("InitialPriceIndex", "100*sum(itmInitialComparableTurnover)/sum(competitorComparableTurnover)"))
        .withMeasure(new ExpressionMeasure("RecommendedPriceIndex", "100*sum(itmRecommendedComparableTurnover)/sum(competitorComparableTurnover)"))
        .withMeasure(new ExpressionMeasure("FinalPriceIndex", "100*sum(itmFinalComparableTurnover)/sum(competitorComparableTurnover)"))

querier.execute(q).then(r => {
  console.log(`Metadata result: ${toString(r.metadata)}`);
  console.log(`Table: ${toString(r.table)}`);
  console.log(`Debug: ${toString(r.debug)}`);
})
