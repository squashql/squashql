import {JoinType, Query, Table} from "./query"
import {
  AggregatedMeasure,
  avgIf,
  Axis,
  BinaryOperationMeasure,
  BinaryOperator,
  comparisonMeasureWithGrandTotal,
  comparisonMeasureWithGrandTotalAlongAncestors,
  comparisonMeasureWithinSameGroup,
  comparisonMeasureWithinSameGroupInOrder,
  comparisonMeasureWithParent,
  comparisonMeasureWithParentOfAxis,
  comparisonMeasureWithPeriod,
  comparisonMeasureWithTotalOfAxis,
  ComparisonMethod,
  decimal,
  ExpressionMeasure,
  integer,
  ParametrizedMeasure,
  sum,
} from "./measure"
import {
  _in,
  all,
  and,
  ConditionType,
  contains,
  criterion,
  criterion_,
  eq,
  ge,
  gt,
  havingCriterion,
  isNotNull,
  isNull,
  like,
  lt,
  or
} from "./condition"
import * as fs from "fs"
import {OrderKeyword} from "./order"
import {GroupColumnSet} from "./columnset"
import {AliasedField, ConstantField, TableField, tableField} from "./field"
import {Month} from "./period"
import {Action, QueryCacheParameter} from "./parameter"
import {countRows, totalCount} from "./index"

export function buildQuery(): Query {
  const table = Table.from("myTable")
  const refTable = Table.from("refTable")
  table.join(refTable, JoinType.INNER, criterion_(new TableField("fromField"), new TableField("toField"), ConditionType.EQ))
  table.join(Table.from("a"), JoinType.LEFT, criterion_(new TableField("a.a_id"), new TableField("myTable.id"), ConditionType.EQ))
  const a = tableField("a")
  const b = tableField("b").as("b_alias")
  const q = new Query()
  q.onTable(table)
          .withColumn(a)
          .withColumn(b)

  const price = new AggregatedMeasure("price.sum", new TableField("price"), "sum")
  q.withMeasure(price)
  const priceFood = new AggregatedMeasure("alias", new TableField("price"), "sum", false, criterion(new TableField("category"), eq("food")))
  q.withMeasure(priceFood)
  const plus = new BinaryOperationMeasure("plusMeasure", BinaryOperator.PLUS, price, priceFood)
  q.withMeasure(plus)
  const relDiff = new BinaryOperationMeasure("relDiff", BinaryOperator.RELATIVE_DIFFERENCE, price, priceFood)
  q.withMeasure(relDiff)
  const expression = new ExpressionMeasure("myExpression", "sum(price*quantity)")
  q.withMeasure(expression)
  q.withMeasure(countRows)
  q.withMeasure(totalCount)
  q.withMeasure(integer(123))
  q.withMeasure(decimal(1.23))

  const f1 = new TableField("myTable.f1")
  const f2 = new TableField("myTable.f2")
  const rate = new TableField("rate")
  const one = new ConstantField(1)
  q.withMeasure(avgIf("whatever", f1.divide(one.plus(rate)), criterion_(f1.plus(f2).as("f1+f2"), one, ConditionType.GT)))

  q.withMeasure(comparisonMeasureWithinSameGroup("comp group", ComparisonMethod.ABSOLUTE_DIFFERENCE, price, new Map([
    [tableField("scenario"), "s-1"]
  ])))
  q.withMeasure(comparisonMeasureWithinSameGroupInOrder("comp group in order", ComparisonMethod.ABSOLUTE_DIFFERENCE, price, new Map([
    [tableField("scenario"), "s-1"]
  ]), ["base", "s1", "s2"]))
  q.withMeasure(comparisonMeasureWithPeriod("growth", ComparisonMethod.DIVIDE, price, new Map([
    [tableField("Year"), "y-1"],
    [tableField("Month"), "m"]
  ]), new Month(tableField("Month"), tableField("Year"))))
  q.withMeasure(comparisonMeasureWithParent("parent", ComparisonMethod.DIVIDE, price, [tableField("Year"), tableField("Month")]))
  q.withMeasure(comparisonMeasureWithGrandTotalAlongAncestors("grandTotalAlongAncestors", ComparisonMethod.DIVIDE, price, [tableField("Year"), tableField("Month")]))
  q.withMeasure(comparisonMeasureWithGrandTotal("grandTotal", ComparisonMethod.DIVIDE, price))
  q.withMeasure(new ParametrizedMeasure("var measure", "VAR", {
    "value": tableField("price"),
    "date": tableField("date"),
    "quantile": 0.95
  }))
  q.withMeasure(new ParametrizedMeasure("incr var measure", "INCREMENTAL_VAR", {
    "value": tableField("price"),
    "date": tableField("date"),
    "quantile": 0.95,
    "ancestors": [tableField("f1"), tableField("f2"), tableField("f3")],
  }))
  q.withMeasure(comparisonMeasureWithParentOfAxis(
          "comp parent of column",
          ComparisonMethod.DIVIDE,
          countRows,
          Axis.COLUMN
  ))
  q.withMeasure(comparisonMeasureWithTotalOfAxis(
          "comp total of row",
          ComparisonMethod.DIVIDE,
          countRows,
          Axis.ROW
  ))

  const queryCondition = or(or(and(eq("a"), eq("b")), lt(5)), like("a%"))
  q.withWhereCriteria(all([
    criterion(new TableField("f1"), queryCondition),
    criterion(new AliasedField("f2"), gt(659)),
    criterion(new TableField("f3"), _in([0, 1, 2])),
    criterion(new TableField("f4"), isNull()),
    criterion(new TableField("f5"), isNotNull()),
    criterion(new TableField("f6"), contains(2))
  ]))

  q.withHavingCriteria(all([
    havingCriterion(price, ge(10)),
    havingCriterion(expression, lt(100)),
  ]))

  q.orderBy(a, OrderKeyword.ASC)
  q.orderByFirstElements(b, ["1", "l", "p"])

  const values = new Map(Object.entries({
    "a": ["a1", "a2"],
    "b": ["b1", "b2"]
  }))
  q.withGroupColumnSet(new GroupColumnSet(tableField("group"), tableField("scenario"), values))

  q.withParameter(new QueryCacheParameter(Action.INVALIDATE))

  // SubQuery - Note this is not valid because a table has been set above, but we are just testing
  // the json here.

  const subQ = new Query()
  subQ.onTable(table)
          .withColumn(tableField("aa"))
          .withColumn(new AliasedField("bb"))
          .withMeasure(sum("sum_aa", new TableField("f")))
  q.table = Table.fromSubQuery(subQ)
  return q
}

export function generateFromQueryDto() {
  const q = buildQuery()
  fs.writeFileSync('json/build-from-querydto.json', JSON.stringify(q))
}
