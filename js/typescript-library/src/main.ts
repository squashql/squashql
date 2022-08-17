import {Query} from "./query";
import {AggregatedMeasure} from "./measures";
import {eq} from "./conditions";

const q = new Query()
q
    .onTable("myTable")
    .withColumn("a")
    .withColumn("b")

q.withMeasure(new AggregatedMeasure("price", "sum"))
q.withMeasure(new AggregatedMeasure("price", "sum", "alias", "category", eq("food")))

console.log(JSON.stringify(q))