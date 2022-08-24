import {PACKAGE} from "./index";
import {Condition} from "./conditions";

export interface Measure {
  readonly class: string
  expression?: string
}

export class AggregatedMeasure implements Measure {
  class: string = PACKAGE + "AggregatedMeasure";
  field: string
  aggregationFunction: string
  alias: string
  expression?: string
  conditionField?: string
  condition?: Condition

  constructor(field: string, aggregationFunction: string, alias: string, conditionField?: string, condition?: Condition) {
    this.field = field
    this.aggregationFunction = aggregationFunction
    this.alias = alias
    this.conditionField = conditionField
    this.condition = condition
  }

  toJSON() {
    return {
      "@class": this.class,
      "field": this.field,
      "aggregationFunction": this.aggregationFunction,
      "alias": this.alias,
      "expression": this.expression,
      "conditionField": this.conditionField,
      "conditionDto": this.condition,
    }
  }
}

export class ExpressionMeasure implements Measure {
  class: string = PACKAGE + "ExpressionMeasure";

  constructor(private alias: string, private sqlExpression: string) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "alias": this.alias,
      "expression": this.sqlExpression,
    }
  }
}

export class BinaryOperationMeasure implements Measure {
  class: string = PACKAGE + "BinaryOperationMeasure";
  alias?: string
  expression?: string
  operator: BinaryOperator
  leftOperand: Measure
  rightOperand: Measure

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

// Helpers

export function sum(alias: string, field: string): Measure {
  return new AggregatedMeasure(field, "sum", alias)
}

export function sumIf(alias: string, field: string, conditionField: string, condition: Condition): Measure {
  return new AggregatedMeasure(field, "sum", alias, conditionField, condition)
}

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
