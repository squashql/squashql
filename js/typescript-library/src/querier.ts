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

  async getMetadata(repoUrl?: string): Promise<MetadataResult> {
    return this.axiosInstance
            .get("/metadata", {
              params: {
                "repo-url": repoUrl
              }
            })
            .then(r => r.data)
  }

  async execute(query: Query): Promise<QueryResult> {
    return this.axiosInstance
            .post("/query", query)
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