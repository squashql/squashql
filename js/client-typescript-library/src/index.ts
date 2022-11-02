import {
  BucketColumnSet,
  ComparisonMethod,
  eq,
  from,
  Querier,
  sum,
  sumIf,
  comparisonMeasureWithPeriod, comparisonMeasureWithBucket
} from "aitm-js-query"

const querier = new Querier("http://localhost:8080");
const assets = "https://raw.githubusercontent.com/paulbares/aitm-assets/main/metrics.json";

const toString = (a: any): string => JSON.stringify(a, null, 1)

querier.getMetadata(assets).then(r => {
  console.log(`Store: ${toString(r.stores)}`);
  console.log(`Measures: ${toString(r.measures)}`)
  console.log(`Agg Func: ${r.aggregationFunctions}`)
})

const amount = sum("amount_sum", "Amount");
const sales = sumIf("sales", "Amount", "IncomeExpense", eq("Revenue"));
const groups = {
  "ABCD": ["A", "B", "C", "D"],
  "BD": ["B", "D"],
  "CDA": ["C", "D", "A"],
}

const bucketColumnSet = new BucketColumnSet("group", "Scenario", new Map(Object.entries(groups)))
const refScenario = {"Scenario": "s-1", "group": "g"}
const amountComparison = comparisonMeasureWithBucket("amount compar. with prev. scenario",
        ComparisonMethod.ABSOLUTE_DIFFERENCE,
        amount,
        new Map(Object.entries(refScenario)));

const q = from("ProjectionScenario")
        .select(
                [],
                [bucketColumnSet],
                [amount, amountComparison, sales])
        .build();

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

querier.execute0(q).then(r => {
  console.log(r);
  // console.log(`Metadata result: ${toString(r.metadata)}`);
  // console.log(`Table: ${toString(r.table)}`);
})
