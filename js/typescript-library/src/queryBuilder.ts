import {Measure} from "./measure"
import {ColumnSet} from "./columnset"
import {JoinType, Query, Table} from "./query"
import Criteria from "./criteria"
import {NullsOrderKeyword, OrderKeyword} from "./order"
import {VirtualTable} from "./virtualtable"
import {Field} from "./field"

export interface CanAddOrderBy {
  orderBy(column: Field, order: OrderKeyword): HasHaving

  orderBy(column: Field, order: OrderKeyword, nullsOrder: NullsOrderKeyword): HasHaving

  orderByFirstElements(column: Field, firstElements: Array<any>): HasHaving
}

export interface CanBeBuildQuery {
  build(): Query
}

export interface CanStartBuildingJoin {
  join(tableName: string, joinType: JoinType): HasStartedBuildingJoin

  joinVirtual(virtualTable: VirtualTable, joinType: JoinType): HasStartedBuildingJoin
}

export interface HasCondition {
  /**
   * Selects columns from a table to be displayed and the measures to compute. Note that the columns and columnSets
   * added to select are automatically added to the groupBy clause of the query: aggregated results are then grouped by
   * the columns and columnSets indicated.
   */
  select(columns: Field[], columnSets: ColumnSet[], measures: Measure[]): CanAddRollup
}

export type HasJoin = HasTable & CanStartBuildingJoin

export interface HasOrderBy extends CanBeBuildQuery {
  limit(limit: number): CanBeBuildQuery
}

export type HasHaving = HasOrderBy & CanAddOrderBy

export interface HasStartedBuildingJoin {
  on(joinCriterion: Criteria): HasJoin
}

export type HasStartedBuildingTable = HasTable & CanStartBuildingJoin

export interface HasTable extends HasCondition {
  where(criterion: Criteria): HasCondition
}

export interface CanAddRollup extends HasOrderBy, CanAddOrderBy, CanAddHaving {
  rollup(columns: Field[]): CanAddHaving
}

export interface CanAddHaving extends HasOrderBy, CanAddOrderBy {
  having(criterion: Criteria): HasHaving
}

export function from(tableName: string): HasStartedBuildingTable {
  const queryBuilder = new QueryBuilder()
  const t = new Table()
  t.name = tableName
  queryBuilder.queryDto.table = t
  return queryBuilder
}

export function fromSubQuery(subQuery: Query): HasStartedBuildingTable {
  const queryBuilder = new QueryBuilder()
  const t = new Table()
  t.subQuery = subQuery
  queryBuilder.queryDto.table = t
  return queryBuilder
}

class QueryBuilder implements HasCondition, HasHaving, HasJoin, HasStartedBuildingTable, HasOrderBy, CanAddRollup {
  readonly queryDto: Query = new Query()
  private currentJoinTableBuilder: JoinTableBuilder = null

  joinVirtual(virtualTable: VirtualTable, joinType: JoinType): HasStartedBuildingJoin {
    this.addJoinToQueryDto()
    this.queryDto.virtualTableDtos.push(virtualTable)
    this.currentJoinTableBuilder = new JoinTableBuilder(this, virtualTable.name, joinType)
    return this.currentJoinTableBuilder
  }

  join(tableName: string, joinType: JoinType): HasStartedBuildingJoin {
    this.addJoinToQueryDto()
    this.currentJoinTableBuilder = new JoinTableBuilder(this, tableName, joinType)
    return this.currentJoinTableBuilder
  }

  private addJoinToQueryDto() {
    const jtb = this.currentJoinTableBuilder
    if (jtb != null) {
      this.queryDto.table.join(Table.from(jtb.tableName), jtb.joinType, jtb.joinCriteria)
      this.currentJoinTableBuilder = null
    }
  }

  select(columns: Field[], columnSets: ColumnSet[], measures: Measure[]): CanAddRollup {
    this.addJoinToQueryDto()
    columns.forEach(c => this.queryDto.withColumn(c))
    columnSets.forEach(cs => this.queryDto.columnSets.set(cs.key, cs))
    measures.forEach(m => this.queryDto.withMeasure(m))
    return this
  }

  rollup(columns: Field[]): CanAddHaving {
    columns.forEach(c => this.queryDto.withRollupColumn(c))
    return this
  }

  where(criterion: Criteria): HasTable {
    this.addJoinToQueryDto()
    this.queryDto.withWhereCriteria(criterion)
    return this
  }

  having(criterion: Criteria): HasHaving {
    this.queryDto.withHavingCriteria(criterion)
    return this
  }

  build(): Query {
    return this.queryDto
  }

  limit(limit: number): CanBeBuildQuery {
    this.queryDto.limit = limit
    return this
  }

  orderBy(column: Field, order: OrderKeyword, nullsOrder: NullsOrderKeyword = NullsOrderKeyword.FIRST): HasHaving {
    this.queryDto.orderBy(column, order, nullsOrder)
    return this
  }

  orderByFirstElements(column: Field, firstElements: Array<any>): HasHaving {
    this.queryDto.orderByFirstElements(column, firstElements)
    return this
  }
}

class JoinTableBuilder implements HasStartedBuildingJoin {

  joinCriteria: Criteria

  constructor(public parent: QueryBuilder, public tableName: string, public joinType: JoinType) {
  }

  on(joinCriterion: Criteria): HasJoin {
    this.joinCriteria = joinCriterion
    return this.parent
  }
}
