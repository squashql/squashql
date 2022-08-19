import {Query} from "./query"
import {AggregatedMeasure} from "./measures"
import {eq} from "./conditions"
import * as fs from "fs"

const q = new Query()
q
    .onTable("myTable")
    .withColumn("a")
    .withColumn("b")

q.withMeasure(new AggregatedMeasure("price", "sum"))
q.withMeasure(new AggregatedMeasure("price", "sum", "alias", "category", eq("food")))

console.log(JSON.stringify(q))

let data = JSON.stringify(q);
fs.writeFileSync('query.json', data);
