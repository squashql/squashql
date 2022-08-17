"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Table = exports.Query = exports.PACKAGE = exports.sayHello = void 0;
function sayHello() {
    console.log('hi');
}
exports.sayHello = sayHello;
exports.PACKAGE = "me.paulbares.query.";
class Query {
    constructor() {
        this.columns = [];
        this.measures = [];
    }
    withColumn(colum) {
        this.columns.push(colum);
        return this;
    }
    onTable(tableName) {
        this.table = new Table(tableName);
        return this;
    }
    withMeasure(measure) {
        this.measures.push(measure);
        return this;
    }
}
exports.Query = Query;
class Table {
    constructor(name) {
        this.name = name;
    }
}
exports.Table = Table;
// export class AggregatedMeasure implements Measure {
//     field: string
//     aggregationFunction: string
//     alias?: string
//     conditionField?: string
//     condition?: Condition
//
//     constructor(field: string, aggregationFunction: string, alias?: string, conditionField?: string, condition?: Condition) {
//         this.field = field
//         this.aggregationFunction = aggregationFunction
//         this.alias = alias
//         this.conditionField = conditionField
//         this.condition = condition
//     }
//
//     class: string = PACKAGE + "AggregatedMeasure";
//
//     toJSON() {
//         return {
//             "@class": this.class,
//             "field": this.field,
//             "aggregationFunction": this.aggregationFunction,
//             "alias": this.alias,
//             "conditionField": this.conditionField,
//             "condition": this.condition,
//         }
//     }
// }
//
// interface Measure {
//     readonly class: string
// }
//
// interface Condition {
//     type: string;
// }
