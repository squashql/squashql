import {Measure} from "./measures";
import {Criteria} from "./conditions";
import {ExplicitOrderDto, Order, OrderKeyword, SimpleOrder} from "./order";
import {BucketColumnSet, ColumnSet, ColumnSetKey} from "./columnsets";

export class Query {
  columns: Array<string>
  rollupColumns: Array<string>
  columnSets: Map<string, ColumnSet>
  measures: Array<Measure>
  table: Table
  criteria: Criteria
  orders: Map<string, Order>
  subQuery: Query
  limit: number = -1

  constructor() {
    this.columns = []
    this.rollupColumns = []
    this.measures = []
    this.criteria = undefined
    this.orders = new Map<string, Order>()
    this.columnSets = new Map<string, ColumnSet>()
  }

  onTable(table: Table): Query {
    this.table = table
    return this
  }

  onVirtualTable(query: Query): Query {
    this.subQuery = query
    return this
  }

  withCriteria(criterion: Criteria): Query {
    this.criteria = criterion
    return this;
  }

  withColumn(colum: string): Query {
    this.columns.push(colum)
    return this
  }

  withRollupColumn(colum: string): Query {
    this.rollupColumns.push(colum)
    return this
  }

  withBucketColumnSet(columSet: BucketColumnSet): Query {
    this.columnSets.set(ColumnSetKey.BUCKET, columSet)
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
      "subQuery": this.subQuery,
      "columns": this.columns,
      "rollupColumns": this.rollupColumns,
      "columnSets": Object.fromEntries(this.columnSets),
      "measures": this.measures,
      "criteriaDto": this.criteria,
      "orders": Object.fromEntries(this.orders),
      "limit": this.limit
    }
  }
}

export class Table {
  private joins: Array<Join> = []

  constructor(public name: string) {
  }

  join(other: Table, type: JoinType, mappings: Array<JoinMapping>) {
    this.joins.push(new Join(other, type, mappings))
  }

  innerJoin(other: Table, from: string, to: string) {
    this.joins.push(new Join(other, JoinType.INNER, [new JoinMapping(this.name, from, other.name, to)]))
  }

  leftJoin(other: Table, from: string, to: string) {
    this.joins.push(new Join(other, JoinType.LEFT, [new JoinMapping(this.name, from, other.name, to)]))
  }
}

export enum JoinType {
  INNER = "INNER",
  LEFT = "LEFT",
}

class Join {
  constructor(private table: Table, private type: JoinType, private mappings: Array<JoinMapping>) {
  }
}

export class JoinMapping {
  constructor(private fromTable: string, private from: string, private toTable: string, private to: string) {
  }
}
