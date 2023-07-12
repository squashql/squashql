import {Query} from "./query";

export interface PivotTableQuery {
  query: Query
  rows: Array<string>
  columns: Array<string>
}

export interface PivotConfig {
  rows: Array<string>
  columns: Array<string>
}
