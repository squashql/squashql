export interface Condition {
  readonly type: ConditionType;
}

enum ConditionType {
  EQ = "EQ",
  NEQ = "NEQ",
  LT = "LT",
  LE = "LE",
  GT = "GT",
  GE = "GE",
  AND = "AND",
  OR = "OR",
}

class SingleValueConditionDto implements Condition {
  constructor(readonly type: ConditionType, private value: any) {
  }
}

class LogicalConditionDto implements Condition {
  constructor(readonly type: ConditionType, private one: Condition, private two: Condition) {
  }
}

export function and(left: Condition, right: Condition) {
  return new LogicalConditionDto(ConditionType.AND, left, right)
}

export function or(left: Condition, right: Condition) {
  return new LogicalConditionDto(ConditionType.OR, left, right)
}

export function eq(value: any) {
  return new SingleValueConditionDto(ConditionType.EQ, value)
}

export function neq(value: any) {
  return new SingleValueConditionDto(ConditionType.NEQ, value)
}

export function lt(value: any) {
  return new SingleValueConditionDto(ConditionType.LT, value)
}

export function le(value: any) {
  return new SingleValueConditionDto(ConditionType.LE, value)
}

export function gt(value: any) {
  return new SingleValueConditionDto(ConditionType.GT, value)
}

export function ge(value: any) {
  return new SingleValueConditionDto(ConditionType.GE, value)
}
