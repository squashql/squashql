import {Parameter} from "./parameters"
import {JoinType, Query} from "./query"

export class QueryMerge {

  private readonly _queries: Array<Query>
  private readonly _joins: Array<JoinType>
  minify?: boolean

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

  withParameter(parameter: Parameter): QueryMerge {
    this._queries.forEach(q => q.withParameter(parameter))
    return this
  }

  toJSON() {
    return {
      "queries": this._queries,
      "joinTypes": this._joins,
      "minify": this.minify,
    }
  }
}
