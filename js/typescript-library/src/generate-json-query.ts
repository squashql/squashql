import * as fs from "fs";
import { BucketColumnSet } from "./columnsets";
import { ConditionType, all, criterion, criterion_, eq, gt, havingCriterion, lt } from "./conditions";
import { tableField, tableFields } from "./field";
import { ExpressionMeasure, avg, sum } from "./measure";
import { OrderKeyword } from "./order";
import { Action, QueryCacheParameter } from "./parameters";
import { JoinType } from "./query";
import { from } from "./queryBuilder";
import { VirtualTable } from "./virtualtable";

export function generateFromQuery() {
  const values = new Map(Object.entries({
    "a": ["a1", "a2"],
    "b": ["b1", "b2"]
  }))
  const cte = new VirtualTable("myCte", ["id", "min", "max", "other"], [[0, 0, 1, "x"], [1, 2, 3, "y"]])
  const bucketColumnSet = new BucketColumnSet(tableField("group"), tableField("scenario"), values)
  const measure = sum("sum", "f1");
  const measureExpr = new ExpressionMeasure("sum_expr", "sum(f1)");
  const fields = tableFields(["a", "b"]);
  const q = from("myTable")
          .join("refTable", JoinType.INNER)
          .on(all([criterion_("myTable.id", "refTable.id", ConditionType.EQ), criterion_("myTable.a", "refTable.a", ConditionType.EQ)]))
          .joinVirtual(cte, JoinType.INNER)
          .on(all([criterion_("myTable.value", "myCte.min", ConditionType.GE), criterion_("myTable.value", "myCte.max", ConditionType.LT)]))
          .where(all([criterion("f2", gt(659)), criterion("f3", eq(123))]))
          .select(fields,
                  [bucketColumnSet],
                  [measure, avg("sum", "f1"), measureExpr])
          .rollup(fields)
          .having(all([havingCriterion(measure, gt(0)), havingCriterion(measureExpr, lt(10))]))
          .orderBy(tableField("f4"), OrderKeyword.ASC)
          .limit(10)
          .build()

  q.withParameter(new QueryCacheParameter(Action.NOT_USE))

  console.log(JSON.stringify(q))
  const data = JSON.stringify(q)
  fs.writeFileSync('build-from-query.json', data)
}
