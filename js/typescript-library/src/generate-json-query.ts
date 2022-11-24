import {from} from "./queryBuilder";
import {all, criterion, eq, gt} from "./conditions";
import {BucketColumnSet, Month} from "./columnsets";
import {avg, comparisonMeasureWithPeriod, ComparisonMethod, sum} from "./measures";
import {OrderKeyword} from "./order";
import * as fs from "fs"

export function generateFromQuery() {
  const values = new Map(Object.entries({
    "a": ["a1", "a2"],
    "b": ["b1", "b2"]
  }))
  const bucketColumnSet = new BucketColumnSet("group", "scenario", values)
  const q = from("myTable")
          .innerJoin("refTable")
          .on("myTable", "id", "refTable", "id")
          .on("myTable", "a", "refTable", "a")
          .where(all([criterion("f2", gt(659)), criterion("f3", eq(123))]))
          .select(["a", "b"],
                  [bucketColumnSet],
                  [sum("sum", "f1"), avg("sum", "f1")])
          .rollup(["a", "b"])
          .orderBy("f4", OrderKeyword.ASC)
          .build()

  console.log(JSON.stringify(q))
  const data = JSON.stringify(q)
  fs.writeFileSync('build-from-query.json', data)
}
