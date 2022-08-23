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
      "aggregation_function": this.aggregationFunction,
      "alias": this.alias,
      "expression": this.expression,
      "condition_field": this.conditionField,
      "condition_dto": this.condition,
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
      "left_operand": this.leftOperand,
      "right_operand": this.rightOperand,
    }
  }
}

export enum BinaryOperator {
  PLUS = "PLUS",
  MINUS = "MINUS",
  MULTIPLY = "MULTIPLY",
  DIVIDE = "DIVIDE",
}


