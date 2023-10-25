export interface Field {
  readonly class: string

  divide(other: Field): Field

  minus(other: Field): Field

  multiply(other: Field): Field

  plus(other: Field): Field
}
