import {Query} from "./query"
import {Field} from "./field"
import {QueryMerge} from "./queryMerge"

export interface PivotTableQuery {
  query: Query
  rows: Array<Field>
  columns: Array<Field>
  hiddenTotals?: Array<Field>
}

export interface PivotTableQueryMerge {
  query: QueryMerge
  rows: Array<Field>
  columns: Array<Field>
}

export interface PivotConfig {
  rows: Array<Field>
  columns: Array<Field>
  hiddenTotals?: Array<Field>
}
