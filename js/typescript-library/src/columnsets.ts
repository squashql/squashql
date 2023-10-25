import PACKAGE from "./package"
import {Field} from "./field"

export interface ColumnSet {
  readonly class: string
  readonly key: string
}

export enum ColumnSetKey {
  BUCKET = "BUCKET",
}

export class BucketColumnSet implements ColumnSet {
  readonly class: string = PACKAGE + "dto.BucketColumnSetDto"
  readonly key: ColumnSetKey = ColumnSetKey.BUCKET

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

export interface Period {
  readonly class: string,
}

export class Month implements Period {
  readonly class: string = PACKAGE + "dto.Period$Month"

  constructor(private month: Field, private year: Field) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "month": this.month,
      "year": this.year,
    }
  }
}

export class Quarter implements Period {
  readonly class: string = PACKAGE + "dto.Period$Quarter"

  constructor(private quarter: Field, private year: Field) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "quarter": this.quarter,
      "year": this.year,
    }
  }
}

export class Semester implements Period {
  readonly class: string = PACKAGE + "dto.Period$Semester"

  constructor(private semester: Field, private year: Field) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "semester": this.semester,
      "year": this.year,
    }
  }
}

export class Year implements Period {
  readonly class: string = PACKAGE + "dto.Period$Year"

  constructor(private year: Field) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "year": this.year,
    }
  }
}
