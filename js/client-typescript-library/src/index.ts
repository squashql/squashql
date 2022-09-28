import {ComparisonMethod, eq, ParentComparisonMeasure, Querier, Query, sum, sumIf, Table} from "aitm-js-query"

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
// const sales = sumIf("sales", "Amount", "Income/Expense", eq("Revenue"));

q.withMeasure(amount)
// q.withMeasure(sales)

// const pop = new ParentComparisonMeasure("percentOfParent", ComparisonMethod.DIVIDE, sales, ["Month", "Year"]);
// q.withMeasure(pop)

// const ebidtaRatio = divide("EBIDTA %", amount, sales);
// q.withMeasure(ebidtaRatio)

// const refPeriod = {"Year": "y-1"}
// const growth = new ComparisonMeasureReferencePosition("Growth", ComparisonMethod.DIVIDE, sales, ColumnSetKey.PERIOD, new Map(Object.entries(refPeriod)));
// q.withMeasure(growth)
// q.withPeriodColumnSet(new PeriodColumnSet(new Year("Year")))

// const kpi = plus("KPI", ebidtaRatio, growth);
// q.withMeasure(kpi)

// const refScenario = {"scenario encrypted": "s-1", "group": "g"}
// const kpiComp = new ComparisonMeasureReferencePosition("KPI comp. with prev. scenario",
//         ComparisonMethod.ABSOLUTE_DIFFERENCE,
//         kpi,
//         ColumnSetKey.BUCKET,
//         new Map(Object.entries(refScenario)));
// q.withMeasure(kpiComp);
// const groups = {
//   "group1": ["A", "B", "C", "D"],
//   "group2": ["A", "D"],
// }
// q.withBucketColumnSet(new BucketColumnSet("group", "scenario encrypted", new Map(Object.entries(groups))));

querier.execute0(q).then(r => {
  console.log(r);
  // console.log(`Metadata result: ${toString(r.metadata)}`);
  // console.log(`Table: ${toString(r.table)}`);
})
