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
