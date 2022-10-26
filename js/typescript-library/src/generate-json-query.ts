import {from} from "./query";
import {eq, gt} from "./conditions";
import {BucketColumnSet, Month, PeriodColumnSet} from "./columnsets";
import {avg, sum} from "./measures";
import {OrderKeyword} from "./order";
import * as fs from "fs"

export function generateFromQuery() {
  const values = new Map(Object.entries({
    "a": ["a1", "a2"],
    "b": ["b1", "b2"]
  }))
  const bucketColumnSet = new BucketColumnSet("group", "scenario", values)
  const periodColumnSet = new PeriodColumnSet(new Month("mois", "annee"));

  const q = from("myTable")
          .innerJoin("refTable")
          .on("myTable", "id", "refTable", "id")
          .on("myTable", "a", "refTable", "a")
          .where("f2", gt(659))
          .where("f3", eq(123))
          .select(["a", "b"],
                  [bucketColumnSet, periodColumnSet],
                  [sum("sum", "f1"), avg("sum", "f1")])
          .orderBy("f4", OrderKeyword.ASC)
          .build()

  console.log(JSON.stringify(q))
  const data = JSON.stringify(q)
  fs.writeFileSync('build-from-query.json', data)
}
