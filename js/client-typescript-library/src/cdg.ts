import {
  JoinMapping,
  JoinType,
  Querier,
  Query,
  Table,
  ExpressionMeasure,
  multiply,
  min,
  avg,
  eq,
  _in
} from "aitm-js-query"

const querier = new Querier("http://localhost:8080");

const toString = (a: any): string => JSON.stringify(a, null, 1)

querier.getMetadata().then(r => {
  console.log(`Store: ${toString(r.stores)}`);
  console.log(`Measures: ${toString(r.measures)}`)
  console.log(`Agg Func: ${r.aggregationFunctions}`)
})
