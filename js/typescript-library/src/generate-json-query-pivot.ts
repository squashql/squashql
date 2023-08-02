import {from} from "./queryBuilder";
import {avg, sum} from "./measure";
import * as fs from "fs"
import {createPivotTableQuery} from "./querier";

export function generateFromQueryPivot() {
  const q = from("myTable")
          .select(["a", "b"],
                  [],
                  [avg("sum", "f1")])
          .build()

  const pivotQuery = createPivotTableQuery(q, {rows: ["a"], columns: ["b"]})
  console.log(JSON.stringify(pivotQuery))
  const data = JSON.stringify(pivotQuery)
  fs.writeFileSync('build-from-query-pivot.json', data)
}
