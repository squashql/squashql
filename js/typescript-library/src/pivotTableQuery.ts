import {Query} from "./query"
import {Field} from "./types/field";

export interface PivotTableQuery {
  query: Query
  rows: Array<Field>
  columns: Array<Field>
}

export interface PivotConfig {
  rows: Array<Field>
  columns: Array<Field>
}
