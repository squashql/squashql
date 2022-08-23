import { AxiosInstance } from "axios";
import { Query } from "./query";
import { Measure } from "./measures";
export declare class Querier {
    private url;
    axiosInstance: AxiosInstance;
    constructor(url: string);
    getMetadata(): Promise<any>;
    execute(query: Query): Promise<QueryResult>;
}
export interface QueryResult {
    table: SimpleTable;
    metadata: MetadataResult;
    debug: any;
}
export interface MetadataResult {
    stores: Array<StoreMetadata>;
    aggregation_functions: Array<string>;
    measures: Array<Measure>;
}
export interface StoreMetadata {
    name: string;
    fields: Array<MetadataItem>;
}
export interface MetadataItem {
    name: string;
    expression: string;
    type: string;
}
export interface SimpleTable {
    columns: Array<string>;
    row: Array<Array<any>>;
}
