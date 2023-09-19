import {JoinMapping, JoinType, Query, Table} from "./query"
import {
  AggregatedMeasure,
  avgIf,
  BinaryOperationMeasure,
  BinaryOperator,
  comparisonMeasureWithBucket,
  comparisonMeasureWithParent,
  comparisonMeasureWithPeriod,
  ComparisonMethod,
  count,
  decimal,
  ExpressionMeasure,
  integer,
  sum,
  totalCount,
} from "./measure"
import {
  _in,
  all,
  and,
  ConditionType,
  criterion,
  eq,
  ge,
  gt,
  havingCriterion,
  isNotNull,
  isNull,
  criterion_,
  like,
  lt,
  or
} from "./conditions"
import * as fs from "fs"
import {OrderKeyword} from "./order";
import {BucketColumnSet, Month} from "./columnsets";
import {ConstantField, TableField} from "./field";

export function generateFromQueryDto() {
  const table = new Table("myTable")
  const refTable = new Table("refTable")
  table.join(refTable, JoinType.INNER, [new JoinMapping(new TableField("fromField"), new TableField("toField"), ConditionType.EQ)])
  table.join(new Table("a"), JoinType.LEFT, [new JoinMapping(new TableField("a.a_id"), new TableField("myTable.id"), ConditionType.EQ)])

  const q = new Query()
  q.onTable(table)
          .withColumn(tableField("a"))
          .withColumn(tableField("b"))

  const price = new AggregatedMeasure("price.sum", new TableField("price"), "sum")
  q.withMeasure(price)
  const priceFood = new AggregatedMeasure("alias", new TableField("price"), "sum", criterion("category", eq("food")))
  q.withMeasure(priceFood)
  const plus = new BinaryOperationMeasure("plusMeasure", BinaryOperator.PLUS, price, priceFood)
  q.withMeasure(plus)
  const expression = new ExpressionMeasure("myExpression", "sum(price*quantity)")
  q.withMeasure(expression)
  q.withMeasure(count)
  q.withMeasure(totalCount)
  q.withMeasure(integer(123))
  q.withMeasure(decimal(1.23))

  const f1 = new TableField("myTable.f1")
  const f2 = new TableField("myTable.f2")
  const rate = new TableField("rate")
  const one = new ConstantField(1)
  q.withMeasure(avgIf("whatever", f1.divide(one.plus(rate)), criterion_(f1.plus(f2), one, ConditionType.GT)))

  q.withMeasure(comparisonMeasureWithBucket("comp bucket", ComparisonMethod.ABSOLUTE_DIFFERENCE, price, new Map(Object.entries({
    "group": "g",
    "scenario": "s-1"
  }))))
  q.withMeasure(comparisonMeasureWithPeriod("growth", ComparisonMethod.DIVIDE, price, new Map(Object.entries({
    "Annee": "y-1",
    "Mois": "m"
  })), new Month("mois", "annee")))
  q.withMeasure(comparisonMeasureWithParent("parent", ComparisonMethod.DIVIDE, price, ["Mois", "Annee"]))

  const queryCondition = or(or(and(eq("a"), eq("b")), lt(5)), like("a%"))
  q.withWhereCriteria(all([
    criterion("f1", queryCondition),
    criterion("f2", gt(659)),
    criterion("f3", _in([0, 1, 2])),
    criterion("f4", isNull()),
    criterion("f5", isNotNull())
  ]))

  q.withHavingCriteria(all([
    havingCriterion(price, ge(10)),
    havingCriterion(expression, lt(100)),
  ]))

  q.orderBy("a", OrderKeyword.ASC)
  q.orderByFirstElements("b", ["1", "l", "p"])

  const values = new Map(Object.entries({
    "a": ["a1", "a2"],
    "b": ["b1", "b2"]
  }))
  q.withBucketColumnSet(new BucketColumnSet("group", "scenario", values))

  // SubQuery - Note this is not valid because a table has been set above, but we are just testing
  // the json here.

  const subQ = new Query()
  subQ.onTable(table)
          .withColumn(tableField("aa"))
          .withMeasure(sum("sum_aa", "f"))
  q.onSubQuery(subQ)

  console.log(JSON.stringify(q))
  const data = JSON.stringify(q)
  fs.writeFileSync('build-from-querydto.json', data)
}
