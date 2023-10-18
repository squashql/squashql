import {BasicMeasure, Field, PACKAGE} from "./index"

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
}

function toJSON(c: Condition) {
  return {
    "@class": c.class,
    "type": c.type,
  }
}

class SingleValueCondition implements Condition {
  class: string = PACKAGE + "dto.SingleValueConditionDto"

  constructor(readonly type: ConditionType, private value: Field) {
  }

  toJSON() {
    return {
      ...toJSON(this),
      "value": this.value,
    }
  }
}

class ConstantCondition implements Condition {
  class: string = PACKAGE + "dto.ConstantConditionDto"

  constructor(readonly type: ConditionType) {
  }

  toJSON() {
    return {
      ...toJSON(this),
    }
  }
}

class InCondition implements Condition {
  type: ConditionType = ConditionType.IN
  class: string = PACKAGE + "dto.InConditionDto"

  constructor(private values: Array<Field>) {
  }

  toJSON() {
    return {
      ...toJSON(this),
      "values": this.values,
    }
  }
}

class LogicalCondition implements Condition {
  class: string = PACKAGE + "dto.LogicalConditionDto"

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

export class Criteria {

  constructor(public field: Field,
              public fieldOther: Field,
              private measure: BasicMeasure,
              private condition: Condition,
              public conditionType: ConditionType,
              public children: Criteria[]) {
  }
}

export function criterion(field: Field, condition: Condition): Criteria {
  return new Criteria(field, undefined, undefined, condition, undefined, undefined)
}

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

export function _in(value: Array<Field>): Condition {
  return new InCondition(value)
}

export function eq(value: Field): Condition {
  return new SingleValueCondition(ConditionType.EQ, value)
}

export function neq(value: Field): Condition {
  return new SingleValueCondition(ConditionType.NEQ, value)
}

export function lt(value: Field): Condition {
  return new SingleValueCondition(ConditionType.LT, value)
}

export function le(value: Field): Condition {
  return new SingleValueCondition(ConditionType.LE, value)
}

export function gt(value: Field): Condition {
  return new SingleValueCondition(ConditionType.GT, value)
}

export function ge(value: Field): Condition {
  return new SingleValueCondition(ConditionType.GE, value)
}

export function like(value: Field): Condition {
  return new SingleValueCondition(ConditionType.LIKE, value)
}
