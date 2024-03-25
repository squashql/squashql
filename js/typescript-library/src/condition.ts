import PACKAGE from "./package"
import Criteria from "./criteria"
import {Field} from "./field"
import {BasicMeasure} from "./measure"

type Primitive = string | number | boolean

export interface Condition {
  readonly class: string
  readonly type: ConditionType
}

export enum ConditionType {
  EQ = "EQ",
  NEQ = "NEQ",
  LT = "LT",
  LE = "LE",
  GT = "GT",
  GE = "GE",
  IN = "IN",
  LIKE = "LIKE",
  AND = "AND",
  OR = "OR",
  NULL = "NULL",
  NOT_NULL = "NOT_NULL",
  ARRAY_CONTAINS = "ARRAY_CONTAINS",
}

function toJSON(c: Condition) {
  return {
    "@class": c.class,
    "type": c.type,
  }
}

/**
 * Generic condition on a string, number or boolean constant.
 */
export class SingleValueCondition implements Condition {
  readonly class: string = PACKAGE + "dto.SingleValueConditionDto"

  constructor(readonly type: ConditionType, private value: Primitive) {
  }

  toJSON() {
    return {
      ...toJSON(this),
      "value": this.value,
    }
  }
}

export class ConstantCondition implements Condition {
  readonly class: string = PACKAGE + "dto.ConstantConditionDto"

  constructor(readonly type: ConditionType) {
  }

  toJSON() {
    return {
      ...toJSON(this),
    }
  }
}

/**
 * In condition on a list of string, number or boolean constants.
 */
export class InCondition implements Condition {
  readonly class: string = PACKAGE + "dto.InConditionDto"
  readonly type: ConditionType = ConditionType.IN

  constructor(private values: Array<Primitive>) {
  }

  toJSON() {
    return {
      ...toJSON(this),
      "values": this.values,
    }
  }
}

export class LogicalCondition implements Condition {
  readonly class: string = PACKAGE + "dto.LogicalConditionDto"

  constructor(readonly type: ConditionType, private one: Condition, private two: Condition) {
  }

  toJSON() {
    return {
      ...toJSON(this),
      "one": this.one,
      "two": this.two,
    }
  }
}

/**
 * Criteria on a single field. The condition can only be based on constants.
 */
export function criterion(field: Field, condition: Condition): Criteria {
  return new Criteria(field, undefined, undefined, condition, undefined, undefined)
}

/**
 * Criteria based on the comparison of 2 fields.
 */
export function criterion_(field: Field, fieldOther: Field, conditionType: ConditionType): Criteria {
  return new Criteria(field, fieldOther, undefined, undefined, conditionType, undefined)
}

export function havingCriterion(measure: BasicMeasure, condition: Condition): Criteria {
  return new Criteria(undefined, undefined, measure, condition, undefined, undefined)
}

export function all(criteria: Criteria[]): Criteria {
  return new Criteria(undefined, undefined, undefined, undefined, ConditionType.AND, criteria)
}

export function any(criteria: Criteria[]): Criteria {
  return new Criteria(undefined, undefined, undefined, undefined, ConditionType.OR, criteria)
}

export function and(left: Condition, right: Condition): Condition {
  return new LogicalCondition(ConditionType.AND, left, right)
}

export function or(left: Condition, right: Condition): Condition {
  return new LogicalCondition(ConditionType.OR, left, right)
}

export function isNull(): Condition {
  return new ConstantCondition(ConditionType.NULL)
}

export function isNotNull(): Condition {
  return new ConstantCondition(ConditionType.NOT_NULL)
}

/**
 * In condition on a list of string, number or boolean constants.
 */
export function _in(value: Array<Primitive>): Condition {
  return new InCondition(value)
}

/**
 * Equal condition on a string, number or boolean constant.
 */
export function eq(value: Primitive): Condition {
  return new SingleValueCondition(ConditionType.EQ, value)
}

/**
 * Not equal condition on a string, number or boolean constant.
 */
export function neq(value: Primitive): Condition {
  return new SingleValueCondition(ConditionType.NEQ, value)
}

/**
 * Lower than condition on a string, number or boolean constant.
 */
export function lt(value: Primitive): Condition {
  return new SingleValueCondition(ConditionType.LT, value)
}

/**
 * Lower or equal condition on a string, number or boolean constant.
 */
export function le(value: Primitive): Condition {
  return new SingleValueCondition(ConditionType.LE, value)
}

/**
 * Greater than condition on a string, number or boolean constant.
 */
export function gt(value: Primitive): Condition {
  return new SingleValueCondition(ConditionType.GT, value)
}

/**
 * Greater or equal condition on a string, number or boolean constant.
 */
export function ge(value: Primitive): Condition {
  return new SingleValueCondition(ConditionType.GE, value)
}

/**
 * Like condition on a string pattern.
 */
export function like(value: string): Condition {
  return new SingleValueCondition(ConditionType.LIKE, value)
}

/**
 * Array contains condition.
 */
export function contains(value: any): Condition {
  return new SingleValueCondition(ConditionType.ARRAY_CONTAINS, value)
}
