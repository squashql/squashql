import Criteria from "./Criteria";
import {TableField} from "./field"
import {serializeMap} from "./util"
import {BasicMeasure, BinaryOperator, ColumnSetKey, Field, Measure, Period} from "./types";
import PACKAGE from "./package";

export class AggregatedMeasure implements BasicMeasure {
  readonly class: string = PACKAGE + "AggregatedMeasure"
  readonly field: Field
  readonly aggregationFunction: string
  readonly alias: string
  readonly expression?: string
  readonly distinct?: boolean
  readonly criteria?: Criteria

  constructor(alias: string, field: Field, aggregationFunction: string, distinct?: boolean, criterion?: Criteria) {
    this.alias = alias
    this.field = field
    this.aggregationFunction = aggregationFunction
    this.distinct = distinct
    this.criteria = criterion
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
  readonly alias: string

  constructor(alias: string, private sqlExpression: string) {
    this.alias = alias
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
  readonly alias: string
  readonly expression?: string
  readonly operator: BinaryOperator
  readonly leftOperand: Measure
  readonly rightOperand: Measure

  constructor(alias: string, operator: BinaryOperator, leftOperand: Measure, rightOperand: Measure) {
    this.alias = alias
    this.operator = operator
    this.leftOperand = leftOperand
    this.rightOperand = rightOperand
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

export const countRows = new AggregatedMeasure("_contributors_count_", new TableField("*"), "count")

export class ComparisonMeasureReferencePosition implements Measure {
  readonly class: string = PACKAGE + "ComparisonMeasureReferencePosition"
  readonly alias: string
  readonly expression?: string

  constructor(alias: string,
              private comparisonMethod: ComparisonMethod,
              private measure: Measure,
              private referencePosition: Map<Field, string>,
              private columnSetKey?: ColumnSetKey,
              private period?: Period,
              private ancestors?: Array<Field>) {
    this.alias = alias
  }

  toJSON() {
    return {
      "@class": this.class,
      "alias": this.alias,
      "comparisonMethod": this.comparisonMethod,
      "measure": this.measure,
      "columnSetKey": this.columnSetKey,
      "period": this.period,
      "referencePosition": this.referencePosition ? Object.fromEntries(serializeMap(this.referencePosition)) : undefined,
      "ancestors": this.ancestors,
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

  constructor(private value: Number) {
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

  constructor(private value: Number) {
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
  return new AggregatedMeasure(alias, field, "count", true);
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
  return new AggregatedMeasure(alias, field, "count", true, criterion);
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

// CONSTANT

export function integer(value: Number): Measure {
  return new LongConstantMeasure(value)
}

export function decimal(value: Number): Measure {
  return new DoubleConstantMeasure(value)
}

// COMPARISON

export function comparisonMeasureWithPeriod(alias: string,
                                            comparisonMethod: ComparisonMethod,
                                            measure: Measure,
                                            referencePosition: Map<Field, string>,
                                            period: Period): Measure {
  return new ComparisonMeasureReferencePosition(alias, comparisonMethod, measure, referencePosition, undefined, period)
}

export function comparisonMeasureWithBucket(alias: string,
                                            comparisonMethod: ComparisonMethod,
                                            measure: Measure,
                                            referencePosition: Map<Field, string>): Measure {
  return new ComparisonMeasureReferencePosition(alias, comparisonMethod, measure, referencePosition, ColumnSetKey.BUCKET)
}

export function comparisonMeasureWithParent(alias: string,
                                            comparisonMethod: ComparisonMethod,
                                            measure: Measure,
                                            ancestors: Array<Field>): Measure {
  return new ComparisonMeasureReferencePosition(alias, comparisonMethod, measure, undefined, undefined, undefined, ancestors)
}
