import axios, {AxiosInstance} from "axios"
import {Query} from "./query"
import {CreateAxiosDefaults} from "axios/index"
import {PivotConfig, PivotTableQuery, PivotTableQueryMerge} from "./pivotTableQuery"
import {Measure} from "./measure"
import {QueryJoin} from "./queryJoin"
import {QueryMerge} from "./queryMerge"

export class Querier {

  axiosInstance: AxiosInstance

  constructor(private url: string, config?: CreateAxiosDefaults) {
    this.axiosInstance = axios.create({
      baseURL: url,
      timeout: 30_000,
      ...config,
      headers: {
        'Accept': 'application/json',
        ...config?.headers
      }
    })
  }

  async getMetadata(): Promise<MetadataResult> {
    return this.axiosInstance
            .get("/metadata")
            .then(r => r.data)
  }

  async expression(measures: Array<Measure>): Promise<Array<Measure>> {
    return this.axiosInstance
            .post("/expression", measures)
            .then(r => r.data)
  }

  async executeQuery(query: Query | QueryMerge): Promise<QueryResult | string> {
    let promise
    switch (query.constructor) {
      case Query:
        promise = this.axiosInstance.post(`/query`, query)
        break
      case QueryMerge:
        promise = this.axiosInstance.post(`/query-merge`, query)
        break
      default:
        throw new Error("Unexpected query type " + query)
    }
    return promise.then(r => r.data)
  }

  async executePivotQuery(query: Query | QueryMerge, pivotConfig: PivotConfig): Promise<PivotTableQueryResult | string> {
    let promise
    switch (query.constructor) {
      case Query:
        promise = this.axiosInstance.post(`/query-pivot`, createPivotTableQuery(<Query>query, pivotConfig))
        break
      case QueryMerge:
        promise = this.axiosInstance.post(`/query-merge-pivot`, createPivotTableQueryMerge(<QueryMerge>query, pivotConfig))
        break
      default:
        throw new Error("Unexpected query type " + query)
    }
    return promise.then(r => r.data)
  }

  async executeQueryJoin(query: QueryJoin): Promise<QueryResult> {
    return this.axiosInstance.post("/experimental/query-join", query)
            .then(r => r.data)
  }
}

export function createPivotTableQuery(query: Query, pivotConfig: PivotConfig): PivotTableQuery {
  return {query, rows: pivotConfig.rows, columns: pivotConfig.columns, hiddenTotals: pivotConfig.hiddenTotals}
}

export function createPivotTableQueryMerge(query: QueryMerge, pivotConfig: PivotConfig): PivotTableQueryMerge {
  return {query, rows: pivotConfig.rows, columns: pivotConfig.columns}
}

export interface QueryResult {
  columns: Array<string>
  cells: Array<Record<string, any>>,
  metadata: Array<MetadataItem>
  debug: any
}

export interface PivotTableQueryResult {
  cells: Array<Record<string, any>>,
  rows: Array<string>
  columns: Array<string>
  hiddenTotals: Array<string>
  values: Array<string>
}

export interface MetadataResult {
  stores: Array<StoreMetadata>
  aggregationFunctions: Array<string>
  measures: Array<Measure>
}

export interface StoreMetadata {
  name: string
  fields: Array<MetadataItem>
}

export interface MetadataItem {
  name: string
  expression: string
  type: string
}

export interface SimpleTable {
  columns: Array<string>
  rows: Array<Array<any>>
}
