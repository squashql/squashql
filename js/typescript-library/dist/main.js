"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const query_1 = require("./query");
const measures_1 = require("./measures");
const conditions_1 = require("./conditions");
const fs = require("fs");
const q = new query_1.Query();
q.onTable("myTable")
    .withColumn("a")
    .withColumn("b");
const price = new measures_1.AggregatedMeasure("price", "sum", "price.sum");
q.withMeasure(price);
const priceFood = new measures_1.AggregatedMeasure("price", "sum", "alias", "category", (0, conditions_1.eq)("food"));
q.withMeasure(priceFood);
const plus = new measures_1.BinaryOperationMeasure("plusMeasure", measures_1.BinaryOperator.PLUS, price, priceFood);
q.withMeasure(plus);
const expression = new measures_1.ExpressionMeasure("myExpression", "sum(price*quantity)");
q.withMeasure(expression);
const queryCondition = (0, conditions_1.or)((0, conditions_1.and)((0, conditions_1.eq)("a"), (0, conditions_1.eq)("b")), (0, conditions_1.lt)(5));
q.withCondition("f1", queryCondition);
q.withCondition("f2", (0, conditions_1.gt)(659));
console.log(JSON.stringify(q));
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
