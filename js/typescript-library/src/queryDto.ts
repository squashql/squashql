import {Measure} from "./measures";
import {Condition} from "./conditions";
import {ExplicitOrderDto, Order, OrderKeyword, SimpleOrder} from "./order";
import {BucketColumnSet, ColumnSet, ColumnSetKey, PeriodColumnSet} from "./columnsets";

export class QueryDto {
  columns: Array<string>
  columnSets: Map<string, ColumnSet>
  measures: Array<Measure>
  table: Table
  conditions: Map<string, Condition>
  orders: Map<string, Order>
  subQuery: QueryDto
  limit: number = -1

  constructor() {
    this.columns = []
    this.measures = []
    this.conditions = new Map<string, Condition>()
    this.orders = new Map<string, Order>()
    this.columnSets = new Map<string, ColumnSet>()
  }

  onTable(table: Table): QueryDto {
    this.table = table
    return this
  }

  onVirtualTable(query: QueryDto): QueryDto {
    this.subQuery = query
    return this
  }

  withCondition(field: string, condition: Condition): QueryDto {
    this.conditions.set(field, condition);
    return this;
  }

  withColumn(colum: string): QueryDto {
    this.columns.push(colum)
    return this
  }

  withBucketColumnSet(columSet: BucketColumnSet): QueryDto {
    this.columnSets.set(ColumnSetKey.BUCKET, columSet)
    return this
  }

  withPeriodColumnSet(columSet: PeriodColumnSet): QueryDto {
    this.columnSets.set(ColumnSetKey.PERIOD, columSet)
    return this
  }

  withMeasure(measure: Measure): QueryDto {
    this.measures.push(measure)
    return this
  }

  orderBy(column: string, order: OrderKeyword): QueryDto {
    this.orders.set(column, new SimpleOrder(order))
    return this
  }

  orderByFirstElements(column: string, firstElements: Array<any>): QueryDto {
    this.orders.set(column, new ExplicitOrderDto(firstElements))
    return this
  }

  toJSON() {
    return {
      "table": this.table,
      "subQuery": this.subQuery,
      "columns": this.columns,
      "columnSets": Object.fromEntries(this.columnSets),
      "measures": this.measures,
      "conditions": Object.fromEntries(this.conditions),
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
