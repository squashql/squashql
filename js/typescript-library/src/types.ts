export interface Field {
  readonly class: string

  divide(other: Field): Field

  minus(other: Field): Field

  multiply(other: Field): Field

  plus(other: Field): Field
}

export interface Measure {
  readonly class: string
  readonly alias: string
  readonly expression?: string
}

export type BasicMeasure = Measure

export interface ColumnSet {
  readonly class: string
  readonly key: string
}

export interface Condition {
  readonly class: string
  readonly type: ConditionType
}

export interface Period {
  readonly class: string,
}

export enum ColumnSetKey {
  BUCKET = "BUCKET",
}

export enum BinaryOperator {
  PLUS = "PLUS",
  MINUS = "MINUS",
  MULTIPLY = "MULTIPLY",
  DIVIDE = "DIVIDE",
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
