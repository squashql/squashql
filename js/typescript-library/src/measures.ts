import {PACKAGE} from "./query";
import {Condition} from "./conditions";

export interface Measure {
    readonly class: string
}

export class AggregatedMeasure implements Measure {
    field: string
    aggregationFunction: string
    alias?: string
    conditionField?: string
    condition?: Condition

    constructor(field: string, aggregationFunction: string, alias?: string, conditionField?: string, condition?: Condition) {
        this.field = field
        this.aggregationFunction = aggregationFunction
        this.alias = alias
        this.conditionField = conditionField
        this.condition = condition
    }

    class: string = PACKAGE + "AggregatedMeasure";

    toJSON() {
        return {
            "@class": this.class,
            "field": this.field,
            "aggregation_function": this.aggregationFunction,
            "alias": this.alias,
            "condition_field": this.conditionField,
            "condition_dto": this.condition,
        }
    }
}