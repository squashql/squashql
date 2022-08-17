import { Condition } from "./conditions";
export interface Measure {
    readonly class: string;
}
export declare class AggregatedMeasure implements Measure {
    field: string;
    aggregationFunction: string;
    alias?: string;
    conditionField?: string;
    condition?: Condition;
    constructor(field: string, aggregationFunction: string, alias?: string, conditionField?: string, condition?: Condition);
    class: string;
    toJSON(): {
        "@class": string;
        field: string;
        aggregation_function: string;
        alias: string;
        condition_field: string;
        condition_dto: Condition;
    };
}
