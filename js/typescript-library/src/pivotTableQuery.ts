import {Query, QueryMerge} from "./query"
import {Field} from "./field"

export interface PivotTableQuery {
  query: Query
  rows: Array<Field>
  columns: Array<Field>
  minify?: boolean
}

export interface PivotTableQueryMerge {
  query: QueryMerge
  rows: Array<Field>
  columns: Array<Field>
  minify?: boolean
}

export interface PivotConfig {
  rows: Array<Field>
  columns: Array<Field>
  minify?: boolean
}
