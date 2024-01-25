import * as fs from "fs"
import {TableField} from "./field"
import {avg, sum} from "./measure"
import {JoinType} from "./query"
import {from} from "./queryBuilder"
import {OrderKeyword, SimpleOrder} from "./order"
import {QueryJoin} from "./queryJoin"
import {all, ConditionType, criterion_} from "./conditions"

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

  const c3 = new TableField("myTable3.c3")
  const query3 = from("myTable3")
          .select([c3], [], [avg("max", new TableField("f3"))])
          .build()

  const orders = new Map([[a, new SimpleOrder(OrderKeyword.ASC)], [c3, new SimpleOrder(OrderKeyword.DESC)]])
  const q = new QueryJoin(query1)
          .join(query2, JoinType.LEFT,
                  all([
                    criterion_(b1, b2, ConditionType.EQ),
                    criterion_(c1, c2, ConditionType.EQ)
                  ]))
          .join(query3, JoinType.INNER)
          .orderBy(orders)
          .limit(12)
  const data = JSON.stringify(q)
  fs.writeFileSync('build-from-query-join.json', data)
}
