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
