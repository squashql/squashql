import {BinaryOperator, Field} from "./types";
import PACKAGE from "./package";

abstract class AField implements Field {
  readonly class: string

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
    super()
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
  tableName: string
  fieldName: string

  constructor(readonly fullName: string) {
    super()
    this.setAttributes()
  }

  private setAttributes(): void {
    if (this.fullName != null) {
      const split = this.fullName.split(".")
      if (split.length > 1) {
        this.tableName = split[0]
        this.fieldName = split[1]
      } else {
        this.fieldName = split[0]
      }
    }
  }


  toJSON() {
    return {
      "@class": this.class,
      "fullName": this.fullName,
      "tableName": this.tableName,
      "fieldName": this.fieldName,
    }
  }
}


export function tableField(fullName:string) {
  return new TableField(fullName)
}

export function tableFields(fullNames:string[]) {
  return fullNames.map(f => new TableField(f))
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
