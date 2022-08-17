"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AggregatedMeasure = void 0;
const query_1 = require("./query");
class AggregatedMeasure {
    constructor(field, aggregationFunction, alias, conditionField, condition) {
        this.class = query_1.PACKAGE + "AggregatedMeasure";
        this.field = field;
        this.aggregationFunction = aggregationFunction;
        this.alias = alias;
        this.conditionField = conditionField;
        this.condition = condition;
    }
    toJSON() {
        return {
            "@class": this.class,
            "field": this.field,
            "aggregation_function": this.aggregationFunction,
            "alias": this.alias,
            "condition_field": this.conditionField,
            "condition_dto": this.condition,
        };
    }
}
exports.AggregatedMeasure = AggregatedMeasure;
