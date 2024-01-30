import {BucketColumnSet, ColumnSet, ColumnSetKey} from "./columnsets"
import {Field} from "./field"
import {Measure} from "./measure"
import {ExplicitOrderDto, Order, OrderKeyword, SimpleOrder} from "./order"
import {Parameter} from "./parameters"
import {VirtualTable} from "./virtualtable"
import {serializeMap} from "./util"
import Criteria from "./criteria"

export class QueryMerge {

  private readonly _queries: Array<Query>
  private readonly _joins: Array<JoinType>

  constructor(query: Query) {
    this._queries = []
    this._joins = []
    this._queries.push(query)
  }

  join(query: Query, joinType: JoinType): QueryMerge {
    this._queries.push(query)
    this._joins.push(joinType)
    return this
  }

  toJSON() {
    return {
      "queries": this._queries,
      "joinTypes": this._joins,
    }
  }
}

export class Query {
  columns: Array<Field>
  rollupColumns: Array<Field>
  columnSets: Map<string, ColumnSet>
  parameters: Map<string, Parameter>
  measures: Array<Measure>
  table: Table
  virtualTables: Array<VirtualTable>
  whereCriteria: Criteria
  havingCriteriaDto: Criteria
  orders: Map<Field, Order>
  subQuery: Query
  limit: number = -1

  constructor() {
    this.columns = []
    this.rollupColumns = []
    this.virtualTables = []
    this.measures = []
    this.whereCriteria = undefined
    this.havingCriteriaDto = undefined
    this.orders = new Map<Field, Order>()
    this.columnSets = new Map<string, ColumnSet>()
    this.parameters = new Map<string, Parameter>()
  }

  onTable(table: Table): Query {
    this.table = table
    return this
  }

  onSubQuery(query: Query): Query {
    this.subQuery = query
    return this
  }

  withWhereCriteria(criterion: Criteria): Query {
    this.whereCriteria = criterion
    return this
  }

  withHavingCriteria(criterion: Criteria): Query {
    this.havingCriteriaDto = criterion
    return this
  }

  withColumn(colum: Field): Query {
    this.columns.push(colum)
    return this
  }

  withRollupColumn(colum: Field): Query {
    this.rollupColumns.push(colum)
    return this
  }

  withBucketColumnSet(columSet: BucketColumnSet): Query {
    this.columnSets.set(ColumnSetKey.BUCKET, columSet)
    return this
  }

  withParameter(parameter: Parameter): Query {
    this.parameters.set(parameter.key, parameter)
    return this
  }

  withMeasure(measure: Measure): Query {
    this.measures.push(measure)
    return this
  }

  orderBy(column: Field, order: OrderKeyword): Query {
    this.orders.set(column, new SimpleOrder(order))
    return this
  }

  orderByFirstElements(column: Field, firstElements: Array<any>): Query {
    this.orders.set(column, new ExplicitOrderDto(firstElements))
    return this
  }

  toJSON() {
    return {
      "table": this.table,
      "subQuery": this.subQuery,
      "virtualTableDtos": this.virtualTables,
      "columns": this.columns,
      "rollupColumns": this.rollupColumns,
      "columnSets": Object.fromEntries(this.columnSets),
      "parameters": Object.fromEntries(this.parameters),
      "measures": this.measures,
      "whereCriteriaDto": this.whereCriteria,
      "havingCriteriaDto": this.havingCriteriaDto,
      "orders": Object.fromEntries(serializeMap(this.orders)),
      "limit": this.limit
    }
  }
}

export class Table {
  joins: Array<Join> = []

  constructor(public name: string) {
  }

  join(other: Table, type: JoinType, criteria: Criteria) {
    this.joins.push(new Join(other, type, criteria))
  }
}

export enum JoinType {
  INNER = "INNER",
  LEFT = "LEFT",
  FULL = "FULL",
  CROSS = "CROSS",
}

class Join {
  constructor(private table: Table, private type: JoinType, private joinCriteria: Criteria) {
  }
}
