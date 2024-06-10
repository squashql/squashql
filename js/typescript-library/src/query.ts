import {ColumnSet, ColumnSetKey, GroupColumnSet} from "./columnset"
import {Field} from "./field"
import {Measure} from "./measure"
import {ExplicitOrder, NullsOrderKeyword, Order, OrderKeyword, SimpleOrder} from "./order"
import {Parameter} from "./parameter"
import {VirtualTable} from "./virtualtable"
import {serializeMap} from "./util"
import Criteria from "./criteria"

export class Query {
  columns: Array<Field>
  rollupColumns: Array<Field>
  columnSets: Map<string, ColumnSet>
  parameters: Map<string, Parameter>
  measures: Array<Measure>
  table: Table
  virtualTableDtos: Array<VirtualTable>
  whereCriteriaDto: Criteria
  havingCriteriaDto: Criteria
  orders: Map<Field, Order>
  limit: number = -1
  minify?: boolean

  constructor() {
    this.columns = []
    this.rollupColumns = []
    this.virtualTableDtos = []
    this.measures = []
    this.whereCriteriaDto = undefined
    this.havingCriteriaDto = undefined
    this.orders = new Map<Field, Order>()
    this.columnSets = new Map<string, ColumnSet>()
    this.parameters = new Map<string, Parameter>()
  }

  onTable(table: Table): Query {
    this.table = table
    return this
  }

  withWhereCriteria(criterion: Criteria): Query {
    this.whereCriteriaDto = criterion
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

  withGroupColumnSet(columSet: GroupColumnSet): Query {
    this.columnSets.set(ColumnSetKey.GROUP, columSet)
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

  orderBy(column: Field, order: OrderKeyword, nullsOrder: NullsOrderKeyword = NullsOrderKeyword.FIRST): Query {
    this.orders.set(column, new SimpleOrder(order, nullsOrder))
    return this
  }

  orderByFirstElements(column: Field, firstElements: Array<any>): Query {
    this.orders.set(column, new ExplicitOrder(firstElements))
    return this
  }

  toJSON() {
    return {
      "table": this.table,
      "virtualTableDtos": this.virtualTableDtos,
      "columns": this.columns,
      "rollupColumns": this.rollupColumns,
      "columnSets": Object.fromEntries(this.columnSets),
      "parameters": Object.fromEntries(this.parameters),
      "measures": this.measures,
      "whereCriteriaDto": this.whereCriteriaDto,
      "havingCriteriaDto": this.havingCriteriaDto,
      "orders": Object.fromEntries(serializeMap(this.orders)),
      "limit": this.limit,
      "minify": this.minify
    }
  }
}

export class Table {
  joins: Array<Join> = []
  name: string
  subQuery: Query

  static from(name: string) {
    const t = new Table()
    t.name = name
    return t
  }

  static fromSubQuery(subQuery: Query) {
    const t = new Table()
    t.subQuery = subQuery
    return t
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
