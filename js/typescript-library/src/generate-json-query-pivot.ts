import * as fs from "fs"
import {TableField, tableField, tableFields} from "./field"
import {avg} from "./measure"
import {createPivotTableQuery} from "./querier"
import {from} from "./queryBuilder"

export function generateFromQueryPivot() {
  const fields = tableFields(["a", "b"])

  const q = from("myTable")
          .select(fields,
                  [],
                  [avg("sum", new TableField("f1"))])
          .build()

  const pivotQuery = createPivotTableQuery(q, {rows: [tableField("a")], columns: [tableField("b")], minify: true})
  const data = JSON.stringify(pivotQuery)
  fs.writeFileSync('build-from-query-pivot.json', data)
}
