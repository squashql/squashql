import {PACKAGE, Period} from "./index"
import {Criteria} from "./conditions"
import {ColumnSetKey} from "./columnsets"
import {Field, TableField} from "./field";
import {toField} from "./util";

// Marker type
export type BasicMeasure = Measure

export interface Measure {
  readonly class: string
  readonly alias: string
  readonly expression?: string
}

export class AggregatedMeasure implements BasicMeasure {
  readonly class: string = PACKAGE + "AggregatedMeasure"
  readonly field: Field
  readonly aggregationFunction: string
  readonly alias: string
  readonly expression?: string
  readonly criteria?: Criteria

  constructor(alias: string, field: Field, aggregationFunction: string, criterion?: Criteria) {
    this.alias = alias
    this.field = field
    this.aggregationFunction = aggregationFunction
    this.criteria = criterion
  }

  toJSON() {
    return {
      "@class": this.class,
      "field": this.field,
      "aggregationFunction": this.aggregationFunction,
      "alias": this.alias,
      "expression": this.expression,
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

export const total_count = new ExpressionMeasure("_total_count_", "COUNT(*) OVER ()")

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

export enum BinaryOperator {
  PLUS = "PLUS",
  MINUS = "MINUS",
  MULTIPLY = "MULTIPLY",
  DIVIDE = "DIVIDE",
}

class CountMeasure extends AggregatedMeasure {
  private static _instance: CountMeasure

  public static get instance() {
    return this._instance || (this._instance = new this("_contributors_count_", new TableField("*"), "count"));
  }
}

export const count = CountMeasure.instance;

class ComparisonMeasureReferencePosition implements Measure {
  readonly class: string = PACKAGE + "ComparisonMeasureReferencePosition"
  readonly alias: string
  readonly expression?: string

  constructor(alias: string,
              private comparisonMethod: ComparisonMethod,
              private measure: Measure,
              private referencePosition: Map<string, string>,
              private columnSetKey?: ColumnSetKey,
              private period?: Period,
              private ancestors?: Array<string>) {
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
      "referencePosition": this.referencePosition ? Object.fromEntries(this.referencePosition) : undefined,
      "ancestors": this.ancestors,
    }
  }
}

export enum ComparisonMethod {
  ABSOLUTE_DIFFERENCE = "ABSOLUTE_DIFFERENCE",
  RELATIVE_DIFFERENCE = "RELATIVE_DIFFERENCE",
  DIVIDE = "DIVIDE",
}

class LongConstantMeasure implements Measure {
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

class DoubleConstantMeasure implements Measure {
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

export function sum(alias: string, field: Field | string): Measure {
  return new AggregatedMeasure(alias, toField(field), "sum")
}

export function min(alias: string, field: Field | string): Measure {
  return new AggregatedMeasure(alias, toField(field), "min")
}

export function max(alias: string, field: Field | string): Measure {
  return new AggregatedMeasure(alias, toField(field), "max")
}

export function avg(alias: string, field: Field | string): Measure {
  return new AggregatedMeasure(alias, toField(field), "avg")
}

// aggIf

export function sumIf(alias: string, field: Field | string, criterion: Criteria): Measure {
  return new AggregatedMeasure(alias, toField(field), "sum", criterion)
}

export function avgIf(alias: string, field: Field | string, criterion: Criteria): Measure {
  return new AggregatedMeasure(alias, toField(field), "avg", criterion)
}

export function minIf(alias: string, field: Field | string, criterion: Criteria): Measure {
  return new AggregatedMeasure(alias, toField(field), "min", criterion)
}

export function maxIf(alias: string, field: Field | string, criterion: Criteria): Measure {
  return new AggregatedMeasure(alias, toField(field), "max", criterion)
}

export function countIf(alias: string, field: Field | string, criterion?: Criteria): Measure {
  return new AggregatedMeasure(alias, toField(field), "count", criterion)
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
  return new LongConstantMeasure(value);
}

export function decimal(value: Number): Measure {
  return new DoubleConstantMeasure(value);
}

// COMPARISON

export function comparisonMeasureWithPeriod(alias: string,
                                            comparisonMethod: ComparisonMethod,
                                            measure: Measure,
                                            referencePosition: Map<string, string>,
                                            period: Period): Measure {
  return new ComparisonMeasureReferencePosition(alias, comparisonMethod, measure, referencePosition, undefined, period)
}

export function comparisonMeasureWithBucket(alias: string,
                                            comparisonMethod: ComparisonMethod,
                                            measure: Measure,
                                            referencePosition: Map<string, string>): Measure {
  return new ComparisonMeasureReferencePosition(alias, comparisonMethod, measure, referencePosition, ColumnSetKey.BUCKET)
}

export function comparisonMeasureWithParent(alias: string,
                                            comparisonMethod: ComparisonMethod,
                                            measure: Measure,
                                            ancestors: Array<string>): Measure {
  return new ComparisonMeasureReferencePosition(alias, comparisonMethod, measure, undefined, undefined, undefined, ancestors)
}
