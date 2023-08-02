import {from} from "./queryBuilder";
import {avg, sum} from "./measure";
import * as fs from "fs"
import {JoinType, QueryMerge} from "./query";

export function generateFromQueryMerge() {
  const query1 = from("myTable")
          .select(["a", "b"], [], [sum("sum", "f1")])
          .build()

  const query2 = from("myTable")
          .select(["a", "b"], [], [avg("sum", "f1")])
          .build()

  const q = new QueryMerge(query1, query2, JoinType.LEFT)

  console.log(JSON.stringify(q))
  const data = JSON.stringify(q)
  fs.writeFileSync('build-from-query-merge.json', data)
}
