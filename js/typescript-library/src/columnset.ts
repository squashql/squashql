import PACKAGE from "./package"
import {Field} from "./field"

export interface ColumnSet {
  readonly class: string
  readonly key: string
}

export enum ColumnSetKey {
  GROUP = "GROUP",
}

export class GroupColumnSet implements ColumnSet {
  readonly class: string = PACKAGE + "dto.GroupColumnSetDto"
  readonly key: ColumnSetKey = ColumnSetKey.GROUP

  constructor(private newField: Field, private field: Field, private values: Map<string, Array<string>>) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "newField": this.newField,
      "field": this.field,
      "values": Object.fromEntries(this.values),
    }
  }
}
