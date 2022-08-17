import { Measure } from "./measures";
export declare function sayHello(): void;
export declare const PACKAGE = "me.paulbares.query.";
export declare class Query {
    columns: Array<string>;
    measures: Array<Measure>;
    table: Table;
    withColumn(colum: string): Query;
    onTable(tableName: string): Query;
    withMeasure(measure: Measure): Query;
}
export declare class Table {
    name: string;
    constructor(name: string);
}
