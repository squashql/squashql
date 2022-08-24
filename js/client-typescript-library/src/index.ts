import {Query, Table, sum, sumIf, eq, Querier} from "aitm-js-query"

const querier = new Querier("http://localhost:8080");
const assets = "https://raw.githubusercontent.com/paulbares/aitm-assets/main/metrics.json";

const toString = (a: any): string => JSON.stringify(a, null, 1)

querier.getMetadata(assets).then(r => {
  console.log(`Store: ${toString(r.stores)}`);
  console.log(`Measures: ${toString(r.measures)}`)
  console.log(`Agg Func: ${r.aggregationFunctions}`)
})


const table = new Table("saas")
const q = new Query()
        .onTable(table)
        .withColumn("scenario encrypted")

q.withMeasure(sum("amount.sum", "Amount"))
q.withMeasure(sumIf("sales", "Amount", "Income/Expense", eq("Revenue")))

querier.execute(q).then(r => {
  console.log(`Metadata result: ${toString(r.metadata)}`);
  console.log(`Table: ${toString(r.table)}`);
})
