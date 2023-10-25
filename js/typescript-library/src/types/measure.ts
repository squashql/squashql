export interface Measure {
  readonly class: string
  readonly alias: string
  readonly expression?: string
}

export type BasicMeasure = Measure

export enum BinaryOperator {
  PLUS = "PLUS",
  MINUS = "MINUS",
  MULTIPLY = "MULTIPLY",
  DIVIDE = "DIVIDE",
}
