import * as fs from "fs"
import {TableField, tableFields} from "./field"
import {avg, max, sum} from "./measure"
import {JoinType} from "./query"
import {from} from "./queryBuilder"
import {Action, QueryCacheParameter} from "./parameter"
import {QueryMerge} from "./queryMerge"

export function generateFromQueryMerge() {
  const fields = tableFields(["a", "b"])
  const query1 = from("myTable")
          .select(fields, [], [sum("sum", new TableField("f1"))])
          .build()

  const query2 = from("myTable")
          .select(fields, [], [avg("avg", new TableField("f1"))])
          .build()

  const query3 = from("myTable")
          .select(fields, [], [max("max", new TableField("f1"))])
          .build()

  const q = new QueryMerge(query1).join(query2, JoinType.LEFT).join(query3, JoinType.INNER).withParameter(new QueryCacheParameter(Action.NOT_USE))
  const data = JSON.stringify(q)
  fs.writeFileSync('json/build-from-query-merge.json', data)
}
