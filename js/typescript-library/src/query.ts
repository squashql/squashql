import {Measure} from "./measures";

export function sayHello() {
    console.log('hi')
}

export const PACKAGE = "me.paulbares.query."

export class Query {
    columns: Array<string> = []
    measures: Array<Measure> = []
    table: Table

    withColumn(colum: string): Query {
        this.columns.push(colum)
        return this
    }

    onTable(tableName: string): Query {
        this.table = new Table(tableName)
        return this
    }

    withMeasure(measure: Measure): Query {
        this.measures.push(measure)
        return this
    }
}

export class Table {
    name: string

    constructor(name: string) {
        this.name = name
    }
}

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