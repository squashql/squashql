import {JoinMapping, JoinType, Query, Table} from "./query"
import {
  AggregatedMeasure,
  BinaryOperationMeasure,
  BinaryOperator,
  ComparisonMeasureReferencePosition,
  ParentComparisonMeasure,
  ComparisonMethod,
  count,
  ExpressionMeasure, sum, integer, decimal,
} from "./measures"
import {_in, and, eq, gt, lt, or, isNull, isNotNull} from "./conditions"
import * as fs from "fs"
import {OrderKeyword} from "./order";
import {BucketColumnSet, ColumnSetKey, Month, PeriodColumnSet} from "./columnsets";

const table = new Table("myTable")
const refTable = new Table("refTable")
table.innerJoin(refTable, "fromField", "toField")
table.join(new Table("a"), JoinType.LEFT, [new JoinMapping("a", "a_id", "myTable", "id")])

const q = new Query()
q.onTable(table)
        .withColumn("a")
        .withColumn("b")

const price = new AggregatedMeasure("price.sum", "price", "sum")
q.withMeasure(price)
const priceFood = new AggregatedMeasure("alias", "price", "sum", "category", eq("food"))
q.withMeasure(priceFood)
const plus = new BinaryOperationMeasure("plusMeasure", BinaryOperator.PLUS, price, priceFood)
q.withMeasure(plus)
const expression = new ExpressionMeasure("myExpression", "sum(price*quantity)")
q.withMeasure(expression)
q.withMeasure(count)
q.withMeasure(integer(123))
q.withMeasure(decimal(1.23))

// Comparisons
q.withMeasure(new ComparisonMeasureReferencePosition("comp bucket", ComparisonMethod.ABSOLUTE_DIFFERENCE, price, ColumnSetKey.BUCKET, new Map(Object.entries({
  "group": "g",
  "scenario": "s-1"
}))))
q.withMeasure(new ComparisonMeasureReferencePosition("growth", ComparisonMethod.DIVIDE, price, ColumnSetKey.PERIOD, new Map(Object.entries({
  "Annee": "y-1",
  "Mois": "m"
}))))

q.withMeasure(new ParentComparisonMeasure("parent", ComparisonMethod.DIVIDE, price, ["Mois", "Annee"]))

const queryCondition = or(and(eq("a"), eq("b")), lt(5));
q.withCondition("f1", queryCondition)
q.withCondition("f2", gt(659))
q.withCondition("f3", _in([0, 1, 2]))
q.withCondition("f4", isNull())
q.withCondition("f5", isNotNull())

q.orderBy("a", OrderKeyword.ASC)
q.orderByFirstElements("b", ["1", "l", "p"])

const values = new Map(Object.entries({
  "a": ["a1", "a2"],
  "b": ["b1", "b2"]
}));
q.withBucketColumnSet(new BucketColumnSet("group", "scenario", values))
q.withPeriodColumnSet(new PeriodColumnSet(new Month("mois", "annee")))

// SubQuery - Note this is not valid because a table has been set above but we are just testing
// the json here.

const subQ = new Query()
subQ.onTable(table)
        .withColumn("aa")
        .withMeasure(sum("sum_aa", "f"))
q.onVirtualTable(subQ)

console.log(JSON.stringify(q))

let data = JSON.stringify(q);
fs.writeFileSync('query.json', data);
