import {from} from "./queryBuilder";
import {all, criterion, eq, gt, havingCriterion, lt} from "./conditions";
import {BucketColumnSet} from "./columnsets";
import {AggregatedMeasure, avg, Measure, sum} from "./measures";
import {OrderKeyword} from "./order";
import * as fs from "fs"

export function generateFromQuery() {
  const values = new Map(Object.entries({
    "a": ["a1", "a2"],
    "b": ["b1", "b2"]
  }))
  const bucketColumnSet = new BucketColumnSet("group", "scenario", values)
  let measure: Measure = sum("sum", "f1");
  const q = from("myTable")
          .innerJoin("refTable")
          .on("myTable", "id", "refTable", "id")
          .on("myTable", "a", "refTable", "a")
          .where(all([criterion("f2", gt(659)), criterion("f3", eq(123))]))
          .select(["a", "b"],
                  [bucketColumnSet],
                  [measure, avg("sum", "f1")])
          .rollup(["a", "b"])
          .having(all([havingCriterion(measure as AggregatedMeasure, gt(0)), havingCriterion(measure as AggregatedMeasure, lt(10))]))
          .orderBy("f4", OrderKeyword.ASC)
          .build()

  console.log(JSON.stringify(q))
  const data = JSON.stringify(q)
  fs.writeFileSync('build-from-query.json', data)
}
