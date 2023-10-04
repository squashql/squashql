import * as fs from "fs"
import {TableField, tableFields} from "./field"
import {avg, sum} from "./measure"
import {JoinType, QueryMerge} from "./query"
import {from} from "./queryBuilder"

export function generateFromQueryMerge() {
  const fields = tableFields(["a", "b"])
  const query1 = from("myTable")
          .select(fields, [], [sum("sum", new TableField("f1"))])
          .build()

  const query2 = from("myTable")
          .select(fields, [], [avg("sum", new TableField("f1"))])
          .build()

  const q = new QueryMerge(query1, query2, JoinType.LEFT)
  const data = JSON.stringify(q)
  fs.writeFileSync('build-from-query-merge.json', data)
}
