import {
  ComparisonMeasure,
  ComparisonMethod,
  divide,
  plus,
  eq,
  Querier,
  Query,
  sum,
  sumIf,
  Table,
  BucketColumnSet,
  PeriodColumnSet,
  Year
} from "aitm-js-query"

const querier = new Querier("http://localhost:8080");
const assets = "https://raw.githubusercontent.com/paulbares/aitm-assets/main/metrics.json";

const toString = (a: any): string => JSON.stringify(a, null, 1)

// querier.getMetadata(assets).then(r => {
//   console.log(`Store: ${toString(r.stores)}`);
//   console.log(`Measures: ${toString(r.measures)}`)
//   console.log(`Agg Func: ${r.aggregationFunctions}`)
// })

const table = new Table("saas")
const q = new Query()
        .onTable(table)
        .withColumn("scenario encrypted")

const amount = sum("amount.sum", "Amount");
const sales = sumIf("sales", "Amount", "Income/Expense", eq("Revenue"));
const ebidtaRatio = divide("EBIDTA %", amount, sales);

q.withMeasure(amount)
q.withMeasure(sales)
q.withMeasure(ebidtaRatio)

const refPeriod = {"Year": "y-1"}
const growth = new ComparisonMeasure("Growth", ComparisonMethod.DIVIDE, sales, "period", new Map(Object.entries(refPeriod)));
q.withMeasure(growth)
q.withPeriodColumnSet(new PeriodColumnSet(new Year("Year")))

const kpi = plus("KPI", ebidtaRatio, growth);
q.withMeasure(kpi)

const refScenario = {"scenario encrypted": "s-1", "group": "g"}
const kpiComp = new ComparisonMeasure("KPI comp. with prev. scenario",
        ComparisonMethod.ABSOLUTE_DIFFERENCE,
        kpi,
        "bucket",
        new Map(Object.entries(refScenario)));
q.withMeasure(kpiComp);
const groups = {
  "group1": ["A", "B", "C", "D"],
  "group2": ["A", "D"],
}
q.withBucketColumnSet(new BucketColumnSet("group", "scenario encrypted", new Map(Object.entries(groups))));

querier.execute(q).then(r => {
  console.log(`Metadata result: ${toString(r.metadata)}`);
  console.log(`Table: ${toString(r.table)}`);
})
