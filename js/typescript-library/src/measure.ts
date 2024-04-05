import PACKAGE from "./package"
import Criteria from "./criteria"
import {serializeMap} from "./util"
import {ColumnSetKey} from "./columnset"
import {Field} from "./field"
import {Period} from "./period"

export interface Measure {
  readonly class: string
  readonly alias: string
  readonly expression?: string
}

export type BasicMeasure = Measure

export class AggregatedMeasure implements BasicMeasure {
  readonly class: string = PACKAGE + "AggregatedMeasure"
  readonly expression?: string

  constructor(readonly alias: string,
              readonly field: Field,
              readonly aggregationFunction: string,
              readonly distinct?: boolean,
              readonly criteria?: Criteria) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "field": this.field,
      "aggregationFunction": this.aggregationFunction,
      "alias": this.alias,
      "expression": this.expression,
      "distinct": this.distinct,
      "criteria": this.criteria,
    }
  }
}

export class ExpressionMeasure implements BasicMeasure {
  readonly class: string = PACKAGE + "ExpressionMeasure"

  constructor(readonly alias: string,
              private sqlExpression: string) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "alias": this.alias,
      "expression": this.sqlExpression,
    }
  }
}

export const totalCount = new ExpressionMeasure("_total_count_", "COUNT(*) OVER ()")

export class BinaryOperationMeasure implements Measure {
  readonly class: string = PACKAGE + "BinaryOperationMeasure"
  readonly expression?: string

  constructor(readonly alias: string,
              readonly operator: BinaryOperator,
              readonly leftOperand: Measure,
              readonly rightOperand: Measure) {
    this.alias = alias
  }

  toJSON() {
    return {
      "@class": this.class,
      "alias": this.alias,
      "operator": this.operator,
      "leftOperand": this.leftOperand,
      "rightOperand": this.rightOperand,
    }
  }
}

export enum BinaryOperator {
  PLUS = "PLUS",
  MINUS = "MINUS",
  MULTIPLY = "MULTIPLY",
  DIVIDE = "DIVIDE",
  RELATIVE_DIFFERENCE = "RELATIVE_DIFFERENCE"
}

export class ComparisonMeasureReferencePosition implements Measure {
  readonly class: string = PACKAGE + "ComparisonMeasureReferencePosition"
  readonly expression?: string

  constructor(readonly alias: string,
              readonly comparisonMethod: ComparisonMethod,
              readonly measure: Measure,
              readonly referencePosition?: Map<Field, string>,
              readonly columnSetKey?: ColumnSetKey,
              readonly elements?: Array<any>,
              readonly period?: Period,
              readonly ancestors?: Array<Field>,
              readonly grandTotalAlongAncestors?: boolean) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "alias": this.alias,
      "comparisonMethod": this.comparisonMethod,
      "measure": this.measure,
      "columnSetKey": this.columnSetKey,
      "elements": this.elements,
      "period": this.period,
      "referencePosition": this.referencePosition ? Object.fromEntries(serializeMap(this.referencePosition)) : undefined,
      "ancestors": this.ancestors,
      "grandTotalAlongAncestors": this.grandTotalAlongAncestors,
    }
  }
}

export class ComparisonMeasureGrandTotal implements Measure {
  readonly class: string = PACKAGE + "ComparisonMeasureGrandTotal"
  readonly expression?: string

  constructor(readonly alias: string,
              readonly comparisonMethod: ComparisonMethod,
              readonly measure: Measure) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "alias": this.alias,
      "comparisonMethod": this.comparisonMethod,
      "measure": this.measure,
    }
  }
}

export class ParametrizedMeasure implements Measure {
  readonly class: string = PACKAGE + "measure.ParametrizedMeasure"
  readonly expression?: string

  constructor(readonly alias: string,
              readonly key: string,
              readonly parameters: Record<string, any>) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "alias": this.alias,
      "key": this.key,
      "parameters": this.parameters,
    }
  }
}

export enum ComparisonMethod {
  ABSOLUTE_DIFFERENCE = "ABSOLUTE_DIFFERENCE",
  RELATIVE_DIFFERENCE = "RELATIVE_DIFFERENCE",
  DIVIDE = "DIVIDE",
}

export class LongConstantMeasure implements Measure {
  readonly class: string = PACKAGE + "LongConstantMeasure"
  readonly alias: string

  constructor(readonly value: number) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "value": this.value,
    }
  }
}

export class DoubleConstantMeasure implements Measure {
  readonly class: string = PACKAGE + "DoubleConstantMeasure"
  readonly alias: string

  constructor(readonly value: number) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "value": this.value,
    }
  }
}

// Helpers

// BASIC agg

export function sum(alias: string, field: Field): Measure {
  return new AggregatedMeasure(alias, field, "sum")
}

export function min(alias: string, field: Field): Measure {
  return new AggregatedMeasure(alias, field, "min")
}

export function max(alias: string, field: Field): Measure {
  return new AggregatedMeasure(alias, field, "max")
}

export function avg(alias: string, field: Field): Measure {
  return new AggregatedMeasure(alias, field, "avg")
}

export function count(alias: string, field: Field): Measure {
  return new AggregatedMeasure(alias, field, "count")
}

export function countDistinct(alias: string, field: Field): Measure {
  return new AggregatedMeasure(alias, field, "count", true)
}


// aggIf

export function sumIf(alias: string, field: Field, criterion: Criteria): Measure {
  return new AggregatedMeasure(alias, field, "sum", false, criterion)
}

export function avgIf(alias: string, field: Field, criterion: Criteria): Measure {
  return new AggregatedMeasure(alias, field, "avg", false, criterion)
}

export function minIf(alias: string, field: Field, criterion: Criteria): Measure {
  return new AggregatedMeasure(alias, field, "min", false, criterion)
}

export function maxIf(alias: string, field: Field, criterion: Criteria): Measure {
  return new AggregatedMeasure(alias, field, "max", false, criterion)
}

export function countIf(alias: string, field: Field, criterion?: Criteria): Measure {
  return new AggregatedMeasure(alias, field, "count", false, criterion)
}

export function countDistinctIf(alias: string, field: Field, criterion?: Criteria): Measure {
  return new AggregatedMeasure(alias, field, "count", true, criterion)
}

// BINARY

export function plus(alias: string, measure1: Measure, measure2: Measure): Measure {
  return new BinaryOperationMeasure(alias, BinaryOperator.PLUS, measure1, measure2)
}

export function minus(alias: string, measure1: Measure, measure2: Measure): Measure {
  return new BinaryOperationMeasure(alias, BinaryOperator.MINUS, measure1, measure2)
}

export function multiply(alias: string, measure1: Measure, measure2: Measure): Measure {
  return new BinaryOperationMeasure(alias, BinaryOperator.MULTIPLY, measure1, measure2)
}

export function divide(alias: string, measure1: Measure, measure2: Measure): Measure {
  return new BinaryOperationMeasure(alias, BinaryOperator.DIVIDE, measure1, measure2)
}

export function relativeDifference(alias: string, measure1: Measure, measure2: Measure): Measure {
  return new BinaryOperationMeasure(alias, BinaryOperator.RELATIVE_DIFFERENCE, measure1, measure2)
}

// CONSTANT

export function integer(value: number): Measure {
  return new LongConstantMeasure(value)
}

export function decimal(value: number): Measure {
  return new DoubleConstantMeasure(value)
}

// COMPARISON

export function comparisonMeasureWithPeriod(alias: string,
                                            comparisonMethod: ComparisonMethod,
                                            measure: Measure,
                                            referencePosition: Map<Field, string>,
                                            period: Period): Measure {
  return new ComparisonMeasureReferencePosition(alias, comparisonMethod, measure, referencePosition, undefined, undefined, period)
}

export function comparisonMeasureWithinSameGroup(alias: string,
                                                 comparisonMethod: ComparisonMethod,
                                                 measure: Measure,
                                                 referencePosition: Map<Field, string>): Measure {
  return new ComparisonMeasureReferencePosition(alias, comparisonMethod, measure, referencePosition)
}

export function comparisonMeasureWithinSameGroupInOrder(alias: string,
                                                        comparisonMethod: ComparisonMethod,
                                                        measure: Measure,
                                                        referencePosition: Map<Field, string>,
                                                        elements: Array<any>): Measure {
  return new ComparisonMeasureReferencePosition(alias, comparisonMethod, measure, referencePosition, undefined, elements)
}

export function comparisonMeasureWithParent(alias: string,
                                            comparisonMethod: ComparisonMethod,
                                            measure: Measure,
                                            ancestors: Array<Field>): Measure {
  return new ComparisonMeasureReferencePosition(alias, comparisonMethod, measure, undefined, undefined, undefined, undefined, ancestors, false)
}

export function comparisonMeasureWithGrandTotalAlongAncestors(alias: string,
                                                              comparisonMethod: ComparisonMethod,
                                                              measure: Measure,
                                                              ancestors: Array<Field>): Measure {
  return new ComparisonMeasureReferencePosition(alias, comparisonMethod, measure, undefined, undefined, undefined, undefined, ancestors, true)
}

export function comparisonMeasureWithGrandTotal(alias: string,
                                                comparisonMethod: ComparisonMethod,
                                                measure: Measure): Measure {
  return new ComparisonMeasureGrandTotal(alias, comparisonMethod, measure)
}
