import axios, {AxiosInstance} from "axios";
import {Query} from "./query";
import {Measure} from "./measures";
import {CreateAxiosDefaults} from "axios/index";

export class Querier {

  axiosInstance: AxiosInstance

  constructor(private url: string, config?: CreateAxiosDefaults) {
    this.axiosInstance = axios.create({
      baseURL: url,
      timeout: 30_000,
      ...config
    });
  }

  async getMetadata(): Promise<MetadataResult> {
    return this.axiosInstance
            .get("/metadata")
            .then(r => r.data)
  }

  async execute(query: Query): Promise<QueryResult> {
    return this.axiosInstance
            .post("/query", query)
            .then(r => r.data)
  }

  async execute0(query: Query): Promise<QueryResult> {
    return this.axiosInstance
            .post("/query-beautify", query)
            .then(r => r.data)
  }

  async expression(measures: Array<Measure>): Promise<Array<Measure>> {
    return this.axiosInstance
            .post("/expression", measures)
            .then(r => r.data)
  }
}

export interface QueryResult {
  table: SimpleTable,
  metadata: Array<MetadataItem>
  debug: any
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
