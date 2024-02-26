import * as fs from "fs"
import {GroupColumnSet} from "./columnsets"
import {all, ConditionType, criterion, criterion_, eq, gt, havingCriterion, lt} from "./conditions"
import {AliasedField, TableField, tableField, tableFields} from "./field"
import {avg, ExpressionMeasure, sum} from "./measure"
import {OrderKeyword} from "./order"
import {Action, QueryCacheParameter} from "./parameters"
import {JoinType} from "./query"
import {from, fromSubQuery} from "./queryBuilder"
import {VirtualTable} from "./virtualtable"

function generateFromQuery() {
  const values = new Map(Object.entries({
    "a": ["a1", "a2"],
    "b": ["b1", "b2"]
  }))
  const cte1 = new VirtualTable("myCte1", ["id", "min", "max", "other"], [[0, 0, 1, "x"], [1, 2, 3, "y"]])
  const cte2 = new VirtualTable("myCte2", ["id", "min", "max", "other"], [[0, 4, 12, "a"], [1, 12, 25, "b"]])
  const groupColumnSet = new GroupColumnSet(tableField("group"), tableField("scenario"), values)
  const measure = sum("sum", new TableField("f1"))
  const measureExpr = new ExpressionMeasure("sum_expr", "sum(f1)")
  const fields = tableFields(["a", "b"])
  const q = from("myTable")
          .join("refTable", JoinType.INNER)
          .on(all([
            criterion_(new TableField("myTable.id"), new TableField("refTable.id"), ConditionType.EQ),
            criterion_(new TableField("myTable.a"), new TableField("refTable.a"), ConditionType.EQ)
          ]))
          .joinVirtual(cte1, JoinType.INNER)
          .on(all([
            criterion_(new TableField("myTable.value"), new TableField("myCte1.min"), ConditionType.GE),
            criterion_(new TableField("myTable.value"), new TableField("myCte1.max"), ConditionType.LT)
          ]))
          .joinVirtual(cte2, JoinType.INNER)
          .on(all([
            criterion_(new TableField("myTable.value"), new TableField("myCte2.min"), ConditionType.GE),
            criterion_(new TableField("myTable.value"), new TableField("myCte2.max"), ConditionType.LT)
          ]))
          .where(all([
            criterion(new TableField("f2"), gt(659)),
            criterion(new TableField("f3"), eq(123))
          ]))
          .select(fields,
                  [groupColumnSet],
                  [measure, avg("sum", new TableField("f1")), measureExpr])
          .rollup(fields)
          .having(all([
            havingCriterion(measure, gt(0)),
            havingCriterion(measureExpr, lt(10))
          ]))
          .orderBy(tableField("f4"), OrderKeyword.ASC)
          .limit(10)
          .build()

  q.minify = false
  q.withParameter(new QueryCacheParameter(Action.NOT_USE))

  const data = JSON.stringify(q)
  fs.writeFileSync('json/build-from-query.json', data)
}

function generateFromSubQuery() {
  const fields = tableFields(["myTable.a", "myTable.b"])
  const sq = from("myTable")
          .select(fields, [], [sum("pnl_sum", new TableField("myTable.pnl"))])
          .build()

  const cte1 = new VirtualTable("myCte1", ["id", "other"], [[0, "x"], [1, "y"]])
  const q = fromSubQuery(sq)
          .joinVirtual(cte1, JoinType.INNER)
          .on(criterion_(new TableField("myTable.a"), new TableField("myCte1.id"), ConditionType.EQ))
          .select(tableFields(["myTable.a", "myCte1.other"]), [], [avg("pnl_avg", new AliasedField("pnl_sum"))])
          .build()

  const data = JSON.stringify(q)
  fs.writeFileSync('json/build-from-subquery.json', data)
}

export function generateJsonQuery() {
  generateFromQuery()
  generateFromSubQuery()
}
