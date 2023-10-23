import axios, {AxiosInstance} from "axios"
import {Query, QueryMerge} from "./query"
import {CreateAxiosDefaults} from "axios/index"
import {PivotConfig, PivotTableQuery} from "./pivotTableQuery"
import {Measure} from "./types";

export class Querier {

  axiosInstance: AxiosInstance

  constructor(private url: string, config?: CreateAxiosDefaults) {
    this.axiosInstance = axios.create({
      baseURL: url,
      timeout: 30_000,
      ...config
    })
  }

  async getMetadata(): Promise<MetadataResult> {
    return this.axiosInstance
            .get("/metadata")
            .then(r => r.data)
  }

  async execute(query: Query, pivotConfig?: PivotConfig, stringify = false): Promise<QueryResult | PivotTableQueryResult | string> {
    let promise
    const urlSuffix = stringify ? "-stringify" : ""
    if (typeof pivotConfig === 'undefined') {
      promise = this.axiosInstance.post(`/query${urlSuffix}`, query)
    } else {
      promise = this.axiosInstance.post(`/query-pivot${urlSuffix}`, createPivotTableQuery(query, pivotConfig))
    }
    return promise.then(r => r.data)
  }

  async executeQueryMerge(query: QueryMerge): Promise<QueryResult> {
    return this.axiosInstance
            .post("/query-merge", query)
            .then(r => r.data)
  }

  async expression(measures: Array<Measure>): Promise<Array<Measure>> {
    return this.axiosInstance
            .post("/expression", measures)
            .then(r => r.data)
  }
}

export function createPivotTableQuery(query: Query, pivotConfig?: PivotConfig): PivotTableQuery {
  return {query, rows: pivotConfig.rows, columns: pivotConfig.columns}
}

export interface QueryResult {
  table: SimpleTable,
  metadata: Array<MetadataItem>
  debug: any
}

export interface PivotTableQueryResult {
  queryResult: QueryResult,
  rows: Array<string>
  columns: Array<string>
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
