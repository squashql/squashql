import {Measure} from "./measures";
import {Condition} from "./conditions";
import {ExplicitOrderDto, Order, OrderKeyword, SimpleOrder} from "./order";
import {BucketColumnSet, ColumnSet, PeriodColumnSet} from "./columnsets";

export class Query {
  columns: Array<string>
  columnSets: Map<string, ColumnSet>
  measures: Array<Measure>
  table: Table
  conditions: Map<string, Condition>
  orders: Map<string, Order>

  constructor() {
    this.columns = []
    this.measures = []
    this.conditions = new Map<string, Condition>()
    this.orders = new Map<string, Order>()
    this.columnSets = new Map<string, ColumnSet>()
  }

  onTable(table: Table): Query {
    this.table = table
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

  withBucketColumnSet(columSet: BucketColumnSet): Query {
    this.columnSets.set("bucket", columSet)
    return this
  }

  withPeriodColumnSet(columSet: PeriodColumnSet): Query {
    this.columnSets.set("period", columSet)
    return this
  }

  withMeasure(measure: Measure): Query {
    this.measures.push(measure)
    return this
  }

  orderBy(column: string, order: OrderKeyword): Query {
    this.orders.set(column, new SimpleOrder(order))
    return this
  }

  orderByFirstElements(column: string, firstElements: Array<any>): Query {
    this.orders.set(column, new ExplicitOrderDto(firstElements))
    return this
  }

  toJSON() {
    return {
      "table": this.table,
      "columns": this.columns,
      "columnSets": Object.fromEntries(this.columnSets),
      "measures": this.measures,
      "conditions": Object.fromEntries(this.conditions),
      "orders": Object.fromEntries(this.orders),
    }
  }
}

export class Table {
  private joins: Array<Join> = []

  constructor(private name: string) {
  }

  join(other: Table, type: JoinType, mapping: JoinMapping) {
    this.joins.push(new Join(other, type, [mapping]))
  }


  innerJoin(other: Table, from: string, to: string) {
    this.joins.push(new Join(other, JoinType.INNER, [new JoinMapping(from, to)]))
  }

  leftJoin(other: Table, from: string, to: string) {
    this.joins.push(new Join(other, JoinType.LEFT, [new JoinMapping(from, to)]))
  }
}

enum JoinType {
  INNER = "inner",
  LEFT = "left",
}

class Join {
  constructor(private table: Table, private type: JoinType, private mappings: Array<JoinMapping>) {
  }
}

class JoinMapping {
  constructor(private from: string, private to: string) {
  }
}
