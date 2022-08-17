"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const query_1 = require("./query");
const measures_1 = require("./measures");
const conditions_1 = require("./conditions");
const q = new query_1.Query();
q
    .onTable("myTable")
    .withColumn("a")
    .withColumn("b");
q.withMeasure(new measures_1.AggregatedMeasure("price", "sum"));
q.withMeasure(new measures_1.AggregatedMeasure("price", "sum", "alias", "category", (0, conditions_1.eq)("food")));
console.log(JSON.stringify(q));
