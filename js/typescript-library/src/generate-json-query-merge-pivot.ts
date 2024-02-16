import * as fs from "fs"
import {TableField, tableField, tableFields} from "./field"
import {avg, sum} from "./measure"
import {createPivotTableQueryMerge} from "./querier"
import {from} from "./queryBuilder"
import {JoinType} from "./query"
import {QueryMerge} from "./queryMerge"

export function generateFromQueryMergePivot() {
  const fields = tableFields(["a", "b"])
  const query1 = from("myTable")
          .select(fields, [], [sum("sum", new TableField("f1"))])
          .build()

  const query2 = from("myTable")
          .select(fields, [], [avg("sum", new TableField("f1"))])
          .build()

  const q = new QueryMerge(query1).join(query2, JoinType.LEFT)
  q.minify = false
  const pivotQuery = createPivotTableQueryMerge(q, {rows: [tableField("a")], columns: [tableField("b")]})
  const data = JSON.stringify(pivotQuery)
  fs.writeFileSync('build-from-query-merge-pivot.json', data)
}
