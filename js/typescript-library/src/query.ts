import {Measure} from "./measures";
import {Condition} from "./conditions";

export function sayHello() {
  console.log('hi')
}

export class Query {
  columns: Array<string>
  measures: Array<Measure>
  table: Table
  conditions: Map<string, Condition>

  constructor() {
    this.columns = []
    this.measures = []
    this.conditions = new Map<string, Condition>()
  }

  onTable(tableName: string): Query {
    this.table = new Table(tableName)
    return this
  }

  withCondition(field: string, condition: Condition): Query {
    this.conditions.set(field, condition);
    return this;
  }

  withColumn(colum: string): Query {
    this.columns.push(colum)
    return this
  }

  withMeasure(measure: Measure): Query {
    this.measures.push(measure)
    return this
  }

  toJSON() {
    return {
      "table": this.table,
      "columns": this.columns,
      "measures": this.measures,
      "conditions": Object.fromEntries(this.conditions),
    }
  }
}

export class Table {
  constructor(private name: string) {
  }
}
