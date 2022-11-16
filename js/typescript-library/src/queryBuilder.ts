import {Measure} from "./measures";
import {ColumnSet} from "./columnsets";
import {JoinMapping, JoinType, Query, Table} from "./query";
import {Condition} from "./conditions";
import {OrderKeyword} from "./order";

interface CanAddOrderBy {
  orderBy(column: string, order: OrderKeyword): HasSelectAndRollup

  orderByFirstElements(column: string, firstElements: Array<any>): HasSelectAndRollup
}

interface CanBeBuildQuery {
  build(): Query
}

interface CanStartBuildingJoin {
  leftOuterJoin(tableName: string): HasStartedBuildingJoin

  innerJoin(tableName: string): HasStartedBuildingJoin
}

interface HasCondition {
  /**
   * Selects columns from a table to be displayed and the measures to compute. Note that the columns and columnSets
   * added to select are automatically added to the groupBy clause of the query: aggregated results are then grouped by
   * the columns and columnSets indicated.
   */
  select(columns: string[], columnSets: ColumnSet[], measures: Measure[]): CanAddRollup
}

type HasJoin = HasTable & HasStartedBuildingJoin & CanStartBuildingJoin

interface HasOrderBy extends CanBeBuildQuery {
  limit(limit: number): CanBeBuildQuery;
}

type HasSelectAndRollup = HasOrderBy & CanAddOrderBy

interface HasStartedBuildingJoin {
  on(fromTable: string, from: string, toTable: string, to: string): HasJoin
}

type HasStartedBuildingTable = HasTable & CanStartBuildingJoin

interface HasTable extends HasCondition {
  where(field: string, condition: Condition): HasTable
}

interface CanAddRollup extends HasOrderBy, CanAddOrderBy {
  rollup(columns: string[]): HasSelectAndRollup
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

class QueryBuilder implements HasCondition, HasSelectAndRollup, HasJoin, HasStartedBuildingTable, HasOrderBy, CanAddRollup {
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

  rollup(columns: string[]): HasSelectAndRollup {
    columns.forEach(c => this.queryDto.withRollupColumn(c))
    return this
  }

  where(field: string, condition: Condition): HasTable {
    this.addJoinToQueryDto()
    this.queryDto.withCondition(field, condition)
    return this
  }

  build(): Query {
    return this.queryDto
  }

  limit(limit: number): CanBeBuildQuery {
    this.queryDto.limit = limit
    return undefined
  }

  orderBy(column: string, order: OrderKeyword): HasSelectAndRollup {
    this.queryDto.orderBy(column, order)
    return this
  }

  orderByFirstElements(column: string, firstElements: Array<any>): HasSelectAndRollup {
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
