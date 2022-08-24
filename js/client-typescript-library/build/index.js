"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const dist_1 = require("aitm-js-query/dist");
const q = new dist_1.Query();
q
    .onTable("myTable")
    .withColumn("a")
    .withColumn("b");
q.withMeasure(new dist_1.AggregatedMeasure("price.sum", "price", "sum"));
q.withMeasure(new dist_1.AggregatedMeasure("price", "sum", "alias", "category", (0, dist_1.eq)("food")));
let querier = new Querier("http://localhost:8080");
console.log(JSON.stringify(q));
