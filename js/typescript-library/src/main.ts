import {Query, Table} from "./query"
import {AggregatedMeasure, BinaryOperationMeasure, BinaryOperator, ExpressionMeasure} from "./measures"
import {_in, and, eq, gt, lt, or} from "./conditions"
import * as fs from "fs"
import {OrderKeyword} from "./order";

const table = new Table("myTable")
const refTable = new Table("refTable")
table.innerJoin(refTable, "fromField", "toField")

const q = new Query()
q.onTable(table)
        .withColumn("a")
        .withColumn("b")

const price = new AggregatedMeasure("price", "sum", "price.sum");
q.withMeasure(price)
const priceFood = new AggregatedMeasure("price", "sum", "alias", "category", eq("food"));
q.withMeasure(priceFood)
const plus = new BinaryOperationMeasure("plusMeasure", BinaryOperator.PLUS, price, priceFood)
q.withMeasure(plus)
const expression = new ExpressionMeasure("myExpression", "sum(price*quantity)")
q.withMeasure(expression)

const queryCondition = or(and(eq("a"), eq("b")), lt(5));
q.withCondition("f1", queryCondition)
q.withCondition("f2", gt(659))
q.withCondition("f3", _in([0, 1, 2]))

q.orderBy("a", OrderKeyword.ASC)
q.orderByFirstElements("b", ["1", "l", "p"])

console.log(JSON.stringify(q))

let data = JSON.stringify(q);
fs.writeFileSync('query.json', data);

// TODO delete
// console.log()
// console.log("METADATA")
// let querier = new Querier("http://localhost:8080");
// querier.getMetadata().then(d => console.log(d))
//
// let q2 = new Query()
//         .onTable("saas")
//         .withColumn("Scenario Name")
//         .withMeasure(new AggregatedMeasure("Amount", "sum", "amount.sum"));
//
// querier.execute(q2)
//         .then(d => {
//           console.log(d.table)
//           console.log(d.metadata)
//         })
