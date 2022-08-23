import axios, {AxiosInstance} from "axios";
import {Query} from "./query";
import {Measure} from "./measures";

export class Querier {

  axiosInstance: AxiosInstance

  constructor(private url: string) {
    this.axiosInstance = axios.create({
      baseURL: url,
      timeout: 10_000,
    });
  }

  async getMetadata(): Promise<any> {
    return this.axiosInstance
            .get("/metadata")
            .then(r => r.data)
  }

  async execute(query: Query): Promise<QueryResult> {
    return this.axiosInstance
            .post("/query", query)
            .then(r => r.data)
  }
}

export interface QueryResult {
  table: SimpleTable,
  metadata: MetadataResult
  debug: any
}

export interface MetadataResult {
  stores: Array<StoreMetadata>
  aggregation_functions: Array<string>
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
  row: Array<Array<any>>
}
