import * as fs from "fs"
import {TableField, tableField, tableFields} from "./field"
import {avg, sum} from "./measure"
import {createPivotTableQueryMerge} from "./querier"
import {from} from "./queryBuilder"
import {JoinType, QueryMerge} from "./query";

export function generateFromQueryMergePivot() {
  const fields = tableFields(["a", "b"])
  const query1 = from("myTable")
      .select(fields, [], [sum("sum", new TableField("f1"))])
      .build()

  const query2 = from("myTable")
      .select(fields, [], [avg("sum", new TableField("f1"))])
      .build()

  const q = new QueryMerge(query1, query2, JoinType.LEFT)

  const pivotQuery = createPivotTableQueryMerge(q, {rows: [tableField("a")], columns: [tableField("b")]})
  const data = JSON.stringify(pivotQuery)
  fs.writeFileSync('build-from-query-merge-pivot.json', data)
}
