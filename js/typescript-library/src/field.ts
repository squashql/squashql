import {PACKAGE} from "./index"
import {BinaryOperator} from "./measure";

export interface Field {
  readonly class: string

  divide(other: Field): Field

  minus(other: Field): Field

  multiply(other: Field): Field

  plus(other: Field): Field
}

abstract class AField implements Field {
  readonly class: string;

  divide(other: Field): Field {
    return new BinaryOperationField(BinaryOperator.DIVIDE, this, other)
  }

  minus(other: Field): Field {
    return new BinaryOperationField(BinaryOperator.MINUS, this, other)
  }

  multiply(other: Field): Field {
    return new BinaryOperationField(BinaryOperator.MULTIPLY, this, other)
  }

  plus(other: Field): Field {
    return new BinaryOperationField(BinaryOperator.PLUS, this, other)
  }
}

export class ConstantField extends AField {
  readonly class: string = PACKAGE + "ConstantField"

  constructor(readonly value: any) {
    super();
  }

  toJSON() {
    return {
      "@class": this.class,
      "value": this.value,
    }
  }
}

export class TableField extends AField {
  readonly class: string = PACKAGE + "TableField"

  constructor(readonly name: any) {
    super();
  }

  toJSON() {
    return {
      "@class": this.class,
      "name": this.name,
    }
  }
}

export class BinaryOperationField extends AField {
  readonly class: string = PACKAGE + "BinaryOperationField"

  constructor(readonly operator: BinaryOperator, readonly leftOperand: Field, readonly rightOperand: Field) {
    super()
  }

  toJSON() {
    return {
      "@class": this.class,
      "operator": this.operator,
      "leftOperand": this.leftOperand,
      "rightOperand": this.rightOperand,
    }
  }
}
