import {PACKAGE} from "./index";

export interface Condition {
  readonly class: string
  readonly type: ConditionType
}

enum ConditionType {
  EQ = "EQ",
  NEQ = "NEQ",
  LT = "LT",
  LE = "LE",
  GT = "GT",
  GE = "GE",
  IN = "IN",
  AND = "AND",
  OR = "OR",
}

function toJSON(c: Condition) {
  return {
    "@class": c.class,
    "type": c.type,
  }
}

class SingleValueCondition implements Condition {
  class: string = PACKAGE + "dto.SingleValueConditionDto";

  constructor(readonly type: ConditionType, private value: any) {
  }

  toJSON() {
    return {
      ...toJSON(this),
      "value": this.value,
    }
  }
}

class InCondition implements Condition {
  type: ConditionType = ConditionType.IN;
  class: string = PACKAGE + "dto.InConditionDto";

  constructor(private values: Array<any>) {
  }

  toJSON() {
    return {
      ...toJSON(this),
      "values": this.values,
    }
  }
}

class LogicalCondition implements Condition {
  class: string = PACKAGE + "dto.LogicalConditionDto";

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

export function and(left: Condition, right: Condition): Condition {
  return new LogicalCondition(ConditionType.AND, left, right)
}

export function or(left: Condition, right: Condition): Condition {
  return new LogicalCondition(ConditionType.OR, left, right)
}

export function _in(value: Array<any>): Condition {
  return new InCondition(value)
}

export function eq(value: any): Condition {
  return new SingleValueCondition(ConditionType.EQ, value)
}

export function neq(value: any): Condition {
  return new SingleValueCondition(ConditionType.NEQ, value)
}

export function lt(value: any): Condition {
  return new SingleValueCondition(ConditionType.LT, value)
}

export function le(value: any): Condition {
  return new SingleValueCondition(ConditionType.LE, value)
}

export function gt(value: any): Condition {
  return new SingleValueCondition(ConditionType.GT, value)
}

export function ge(value: any): Condition {
  return new SingleValueCondition(ConditionType.GE, value)
}
