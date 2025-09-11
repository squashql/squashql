import {JoinType, Query, Table} from "./query"
import {serializeMap} from "./util"
import {Field} from "./field"
import {Order} from "./order"
import Criteria from "./criteria"

export class QueryJoin {

  minify?: boolean
  private readonly _table: Table
  private readonly _queries: Array<Query>
  private current: number = 0
  private _where: Criteria | null = null
  private _orders: Map<Field, Order>
  private _limit: number = -1

  constructor(query: Query) {
    this._queries = []
    this._queries.push(query)
    this._table = Table.from(`__cte${this.current++}__`)
    this._orders = new Map()
  }

  join(query: Query, joinType: JoinType, criteria?: Criteria): QueryJoin {
    this._queries.push(query)
    this._table.join(Table.from(`__cte${this.current++}__`), joinType, criteria)
    return this
  }

  where(where: Criteria) {
    this._where = where
    return this
  }

  orderBy(orders: Map<Field, Order>): QueryJoin {
    this._orders = orders
    return this
  }

  limit(limit: number) {
    this._limit = limit
    return this
  }

  toJSON() {
    return {
      "table": this._table,
      "queries": this._queries,
      "minify": this.minify,
      "where": this._where,
      "orders": Object.fromEntries(serializeMap(this._orders)),
      "limit": this._limit,
    }
  }
}
