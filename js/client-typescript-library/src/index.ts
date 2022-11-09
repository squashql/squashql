import {
  ComparisonMethod,
  from,
  sum,
  Semester,
  comparisonMeasureWithPeriod,
  Querier,
} from "aitm-js-query"

const querier = new Querier("http://localhost:8080");
const assets = "https://raw.githubusercontent.com/paulbares/aitm-assets/main/metrics.json";

const toString = (a: any): string => JSON.stringify(a, null, 1)

querier.getMetadata(assets).then(r => {
  console.log(`Store: ${toString(r.stores)}`);
  console.log(`Measures: ${toString(r.measures)}`)
  console.log(`Agg Func: ${r.aggregationFunctions}`)
})

const scoreSum = sum("score_sum", "score");
const comparisonScore = comparisonMeasureWithPeriod(
        "compare with previous year",
        ComparisonMethod.ABSOLUTE_DIFFERENCE,
        scoreSum,
        new Map(Object.entries({"semester": "s-1", "year": "y"})),
        new Semester("semester", "year"));

const q = from("student")
        .select(["year", "semester", "name"], [], [scoreSum, comparisonScore])
        .build();

// querier.execute0(q).then(r => {
//   console.log(r);
//   // console.log(`Metadata result: ${toString(r.metadata)}`);
//   // console.log(`Table: ${toString(r.table)}`);
// })
