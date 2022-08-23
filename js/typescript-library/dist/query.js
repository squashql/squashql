"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Table = exports.Query = exports.sayHello = void 0;
function sayHello() {
    console.log('hi');
}
exports.sayHello = sayHello;
class Query {
    constructor() {
        this.columns = [];
        this.measures = [];
        this.conditions = new Map();
    }
    onTable(tableName) {
        this.table = new Table(tableName);
        return this;
    }
    withCondition(field, condition) {
        this.conditions.set(field, condition);
        return this;
    }
    withColumn(colum) {
        this.columns.push(colum);
        return this;
    }
    withMeasure(measure) {
        this.measures.push(measure);
        return this;
    }
    toJSON() {
        return {
            "table": this.table,
            "columns": this.columns,
            "measures": this.measures,
            "conditions": Object.fromEntries(this.conditions),
        };
    }
}
exports.Query = Query;
class Table {
    constructor(name) {
        this.name = name;
    }
}
exports.Table = Table;
