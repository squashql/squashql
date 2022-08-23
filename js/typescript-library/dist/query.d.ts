import { Measure } from "./measures";
import { Condition } from "./conditions";
export declare function sayHello(): void;
export declare class Query {
    columns: Array<string>;
    measures: Array<Measure>;
    table: Table;
    conditions: Map<string, Condition>;
    constructor();
    onTable(tableName: string): Query;
    withCondition(field: string, condition: Condition): Query;
    withColumn(colum: string): Query;
    withMeasure(measure: Measure): Query;
    toJSON(): {
        table: Table;
        columns: string[];
        measures: Measure[];
        conditions: {
            [k: string]: Condition;
        };
    };
}
export declare class Table {
    private name;
    constructor(name: string);
}
