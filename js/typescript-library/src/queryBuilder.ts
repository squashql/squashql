import {Measure} from "./measures";
import {ColumnSet} from "./columnsets";
import {JoinMapping, JoinType, Query, Table} from "./query";
import {Criteria} from "./conditions";
import {OrderKeyword} from "./order";

export interface CanAddOrderBy {
  orderBy(column: string, order: OrderKeyword): HasHaving

  orderByFirstElements(column: string, firstElements: Array<any>): HasHaving
}

export interface CanBeBuildQuery {
  build(): Query
}

export interface CanStartBuildingJoin {
  leftOuterJoin(tableName: string): HasStartedBuildingJoin

  innerJoin(tableName: string): HasStartedBuildingJoin
}

export interface HasCondition {
  /**
   * Selects columns from a table to be displayed and the measures to compute. Note that the columns and columnSets
   * added to select are automatically added to the groupBy clause of the query: aggregated results are then grouped by
   * the columns and columnSets indicated.
   */
  select(columns: string[], columnSets: ColumnSet[], measures: Measure[]): CanAddRollup
}

export type HasJoin = HasTable & HasStartedBuildingJoin & CanStartBuildingJoin

export interface HasOrderBy extends CanBeBuildQuery {
  limit(limit: number): CanBeBuildQuery;
}

export type HasHaving = HasOrderBy & CanAddOrderBy

export interface HasStartedBuildingJoin {
  on(fromTable: string, from: string, toTable: string, to: string): HasJoin
}

export type HasStartedBuildingTable = HasTable & CanStartBuildingJoin

export interface HasTable extends HasCondition {
  where(criterion: Criteria): HasCondition
}

export interface CanAddRollup extends HasOrderBy, CanAddOrderBy, CanAddHaving {
  rollup(columns: string[]): CanAddHaving
}

export interface CanAddHaving extends HasOrderBy, CanAddOrderBy {
  having(criterion: Criteria): HasHaving
}

export function from(tableName: string): HasStartedBuildingTable {
  const queryBuilder = new QueryBuilder();
  queryBuilder.queryDto.table = new Table(tableName)
  return queryBuilder
}

export function fromSubQuery(subQuery: Query): HasStartedBuildingTable {
  const queryBuilder = new QueryBuilder();
  queryBuilder.queryDto.subQuery = subQuery
  return queryBuilder
}

class QueryBuilder implements HasCondition, HasHaving, HasJoin, HasStartedBuildingTable, HasOrderBy, CanAddRollup {
  readonly queryDto: Query = new Query()
  private currentJoinTableBuilder: JoinTableBuilder = null;

  on(fromTable: string, from: string, toTable: string, to: string): HasJoin {
    this.currentJoinTableBuilder.on(fromTable, from, toTable, to)
    return this
  }

  innerJoin(tableName: string): HasStartedBuildingJoin {
    return this.join(tableName, JoinType.INNER)
  }

  leftOuterJoin(tableName: string): HasStartedBuildingJoin {
    return this.join(tableName, JoinType.LEFT)
  }

  private join(tableName: string, joinType: JoinType): HasStartedBuildingJoin {
    this.addJoinToQueryDto()
    this.currentJoinTableBuilder = new JoinTableBuilder(this, tableName, joinType)
    return this.currentJoinTableBuilder
  }

  private addJoinToQueryDto() {
    const jtb = this.currentJoinTableBuilder
    if (jtb != null) {
      this.queryDto.table.join(new Table(jtb.tableName), jtb.joinType, jtb.mappings)
      this.currentJoinTableBuilder = null
    }
  }

  select(columns: string[], columnSets: ColumnSet[], measures: Measure[]): CanAddRollup {
    this.addJoinToQueryDto()
    columns.forEach(c => this.queryDto.withColumn(c))
    columnSets.forEach(cs => this.queryDto.columnSets.set(cs.key, cs))
    measures.forEach(m => this.queryDto.withMeasure(m))
    return this
  }

  rollup(columns: string[]): CanAddHaving {
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

  orderBy(column: string, order: OrderKeyword): HasHaving {
    this.queryDto.orderBy(column, order)
    return this
  }

  orderByFirstElements(column: string, firstElements: Array<any>): HasHaving {
    this.queryDto.orderByFirstElements(column, firstElements)
    return this
  }
}

class JoinTableBuilder implements HasStartedBuildingJoin {

  readonly mappings: Array<JoinMapping> = []

  constructor(public parent: QueryBuilder, public tableName: string, public joinType: JoinType) {
  }

  on(fromTable: string, from: string, toTable: string, to: string): HasJoin {
    this.mappings.push(new JoinMapping(fromTable, from, toTable, to))
    return this.parent
  }
}
