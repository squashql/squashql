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
