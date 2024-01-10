import * as fs from "fs"
import {TableField} from "./field"
import {avg, sum} from "./measure"
import {JoinType, QueryJoin} from "./query"
import {from} from "./queryBuilder"
import {all, ConditionType, criterion_} from "./conditions"
import {OrderKeyword, SimpleOrder} from "./order"

export function generateFromQueryJoin() {
  const a = new TableField("myTable1.a")
  const b1 = new TableField("myTable1.b")
  const c1 = new TableField("myTable1.c")
  const query1 = from("myTable1")
          .select([a, b1, c1], [], [sum("sum", new TableField("f1"))])
          .build()

  const b2 = new TableField("myTable2.b")
  const c2 = new TableField("myTable2.c")
  const query2 = from("myTable2")
          .select([b2, c2], [], [avg("sum", new TableField("f2"))])
          .build()

  const q = new QueryJoin(query1, query2, JoinType.LEFT,
          all([
            criterion_(b1, b2, ConditionType.EQ),
            criterion_(c1, c2, ConditionType.EQ)
          ]),
          new Map([[a, new SimpleOrder(OrderKeyword.ASC)]]),
          10)
  const data = JSON.stringify(q)
  fs.writeFileSync('build-from-query-join.json', data)
}
