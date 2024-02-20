import * as fs from "fs"
import {PivotTableQueryResult, QueryResult} from "./querier"

function generateFromQueryResult() {
  const r: QueryResult = {
    columns: ["key1", "key2"],
    cells: [
      {key1: 1, key2: "sthg"},
      {key1: 2, key2: "sthg else"},
    ],
    metadata: [{
      name: "key1",
      expression: "key1",
      type: "int",
    }, {
      name: "key2",
      expression: "key2",
      type: "java.lang.String",
    }],
    debug: undefined,
  }
  fs.writeFileSync('json/build-from-query-result.json', JSON.stringify(r))
}

function generateFromPivotQueryResult() {
  const r: PivotTableQueryResult = {
    cells: [
      {key1: 1, key2: "sthg", key3: true, key4: 123.5},
      {key1: 2, key2: "sthg else", key3: false, key4: 321.5},
    ],
    rows: ["key1", "key2"],
    columns: ["key3"],
    values: ["key4"]
  }
  fs.writeFileSync('json/build-from-pivot-query-result.json', JSON.stringify(r))
}


export function generateQueryResults() {
  generateFromQueryResult()
  generateFromPivotQueryResult()
}
