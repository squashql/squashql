import {Measure} from "./measure";
import {ConditionType, Criteria} from "./conditions";
import {ExplicitOrderDto, Order, OrderKeyword, SimpleOrder} from "./order";
import {BucketColumnSet, ColumnSet, ColumnSetKey} from "./columnsets";
import { VirtualTable } from "./virtualtable";
import {Parameter} from "./parameters";
import {Field} from "./field";

export class QueryMerge {
  constructor(readonly first: Query, readonly second: Query, readonly joinType: JoinType) {
  }
}

export class Query {
  columns: Array<string>
  rollupColumns: Array<string>
  columnSets: Map<string, ColumnSet>
  parameters: Map<string, Parameter>
  measures: Array<Measure>
  table: Table
  virtualTable: VirtualTable
  whereCriteria: Criteria
  havingCriteriaDto: Criteria
  orders: Map<string, Order>
  subQuery: Query
  limit: number = -1

  constructor() {
    this.columns = []
    this.rollupColumns = []
    this.measures = []
    this.whereCriteria = undefined
    this.havingCriteriaDto = undefined
    this.orders = new Map<string, Order>()
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
    return this;
  }

  withHavingCriteria(criterion: Criteria): Query {
    this.havingCriteriaDto = criterion
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

  withParameter(parameter: Parameter): Query {
    this.parameters.set(parameter.key, parameter)
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
      "virtualTableDto": this.virtualTable,
      "columns": this.columns,
      "rollupColumns": this.rollupColumns,
      "columnSets": Object.fromEntries(this.columnSets),
      "parameters": Object.fromEntries(this.parameters),
      "measures": this.measures,
      "whereCriteriaDto": this.whereCriteria,
      "havingCriteriaDto": this.havingCriteriaDto,
      "orders": Object.fromEntries(this.orders),
      "limit": this.limit
    }
  }
}

export class Table {
  joins: Array<Join> = []

  constructor(public name: string) {
  }

  join(other: Table, type: JoinType, mappings: Array<JoinMapping>) {
    this.joins.push(new Join(other, type, mappings))
  }
}

export enum JoinType {
  INNER = "INNER",
  LEFT = "LEFT",
  FULL = "FULL",
}

class Join {
  constructor(private table: Table, private type: JoinType, private mappings: Array<JoinMapping>) {
  }
}

export class JoinMapping {
  constructor(private from: Field, private to: Field, private conditionType: ConditionType) {
  }
}
